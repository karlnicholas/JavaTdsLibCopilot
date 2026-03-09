package org.tdslib.javatdslib.tokens;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

public class StatefulTokenDecoder implements TdsStreamHandler {
  private static final Logger logger = LoggerFactory.getLogger(StatefulTokenDecoder.class);

  private final TokenParserRegistry registry;
  private final ConnectionContext context;
  private final TokenVisitor visitor;
  private final TdsTransport transport;

  private ByteBuffer accumulator = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);

  private boolean expectingNewToken = true;
  private byte currentTokenType = 0;

  // FIX: Make thread-safe since the Mapper Worker thread triggers the re-entrant calls
  private final AtomicBoolean isParsing = new AtomicBoolean(false);
  private ByteBuffer pendingReentrantBytes = null;

  public StatefulTokenDecoder(
      TokenParserRegistry registry,
      ConnectionContext context,
      TokenVisitor visitor,
      TdsTransport transport) {
    this.registry = registry;
    this.context = context;
    this.visitor = visitor;
    this.transport = transport;
  }

  @Override
  public void onPayloadAvailable(ByteBuffer chunk, boolean isEom) {
    logger.trace("[StatefulTokenDecoder] Received chunk of {} bytes. isEom: {}, isParsing: {}", chunk.remaining(), isEom, isParsing.get());

    if (!isParsing.compareAndSet(false, true)) {
      logger.trace("[StatefulTokenDecoder] Re-entrant call detected! Saving {} bytes.", chunk.remaining());
      if (pendingReentrantBytes == null) {
        pendingReentrantBytes = ByteBuffer.allocate(chunk.remaining()).order(ByteOrder.LITTLE_ENDIAN);
      } else if (pendingReentrantBytes.remaining() < chunk.remaining()) {
        ByteBuffer expanded = ByteBuffer.allocate(pendingReentrantBytes.capacity() + chunk.remaining() + 1024).order(ByteOrder.LITTLE_ENDIAN);
        pendingReentrantBytes.flip();
        expanded.put(pendingReentrantBytes);
        pendingReentrantBytes = expanded;
      }
      pendingReentrantBytes.put(chunk);
      return;
    }

    try {
      if (accumulator.remaining() < chunk.remaining()) {
        ByteBuffer newAcc = ByteBuffer.allocate(accumulator.capacity() + chunk.remaining() + 4096).order(ByteOrder.LITTLE_ENDIAN);
        accumulator.flip();
        newAcc.put(accumulator);
        accumulator = newAcc;
      }
      accumulator.put(chunk);
      accumulator.flip();

      while (accumulator.hasRemaining()) {
        if (expectingNewToken) {
          currentTokenType = accumulator.get();
          expectingNewToken = false;
        }

        if (currentTokenType == TokenType.ROW.getValue()) {
          logger.trace("[StatefulTokenDecoder] Handing over to processRowToken...");

          // FIX: Slice the exact remaining payload and force Little Endian
          ByteBuffer rowPayload = accumulator.slice().order(ByteOrder.LITTLE_ENDIAN);

          if (!processRowToken(rowPayload, isEom)) {
            logger.trace("[StatefulTokenDecoder] processRowToken needed more bytes. Breaking loop.");
            break;
          }

          logger.trace("[StatefulTokenDecoder] ROW token emitted. Protocol-Driven TCP Backpressure invoked.");
          transport.suspendNetworkRead();
          expectingNewToken = true;

          // FIX: Transfer ownership. Advance our cursor to the end so compact() clears the array.
          accumulator.position(accumulator.limit());
          break;
        }

        accumulator.mark();
        TokenParser parser = registry.getParser(currentTokenType);

        if (parser == null) {
          String msg = String.format("Protocol Violation: Unknown TDS token type: 0x%02X", currentTokenType);
          visitor.onError(new IllegalStateException(msg));
          return;
        }

        try {
          Token token = parser.parse(accumulator, currentTokenType, context);
          visitor.onToken(token);
          expectingNewToken = true;
        } catch (java.nio.BufferUnderflowException e) {
          accumulator.reset();
          break;
        } catch (Exception e) {
          logger.error("Error parsing token type 0x{:02X}", currentTokenType, e);
          visitor.onError(e);
          return;
        }
      }
      accumulator.compact();
      logger.trace("[StatefulTokenDecoder] Loop finished. Accumulator compacted. Remaining bytes: {}", accumulator.position());
    } finally {
      isParsing.set(false);
      if (pendingReentrantBytes != null) {
        logger.trace("[StatefulTokenDecoder] Processing {} pending re-entrant bytes after main loop.", pendingReentrantBytes.position());
        pendingReentrantBytes.flip();
        ByteBuffer toProcess = pendingReentrantBytes;
        pendingReentrantBytes = null;
        onPayloadAvailable(toProcess, isEom);
      }
    }
  }

  private boolean processRowToken(ByteBuffer buffer, boolean isEom) {
    try {
      buffer.mark();
      TokenParser parser = registry.getParser(TokenType.ROW.getValue());
      Token token = parser.parse(buffer, currentTokenType, context);
      visitor.onToken(token);
      return true;
    } catch (java.nio.BufferUnderflowException e) {
      logger.warn("ROW parser underflowed! Buffer pos: {}, limit: {}, remaining bytes: {}, EOM is {}",
          buffer.position(), buffer.limit(), buffer.remaining(), isEom, e);
      buffer.reset();
      return false;
    } catch (Exception e) {
      logger.error("STATEFUL: Error parsing ROW token", e);
      throw e;
    }
  }
}