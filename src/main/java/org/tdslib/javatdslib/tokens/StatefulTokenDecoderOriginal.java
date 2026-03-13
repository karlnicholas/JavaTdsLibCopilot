package org.tdslib.javatdslib.tokens;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatefulTokenDecoderOriginal implements TdsStreamHandler {
  private static final Logger logger = LoggerFactory.getLogger(StatefulTokenDecoderOriginal.class);

  private final TokenParserRegistry registry;
  private final ConnectionContext context;
  private final TokenVisitor visitor;
  private final TdsTransport transport;

  private ByteBuffer accumulator = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);

  private boolean expectingNewToken = true;
  private byte currentTokenType = 0;

  // NEW: Save the active metadata blueprint for the current result set
  private ColMetaDataToken currentMetaData;

  private final AtomicBoolean isParsing = new AtomicBoolean(false);
  private ByteBuffer pendingReentrantBytes = null;

  public StatefulTokenDecoderOriginal(
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
    logger.trace("Received chunk of {} bytes. isEom: {}, isParsing: {}", chunk.remaining(), isEom, isParsing.get());

    if (!isParsing.compareAndSet(false, true)) {
      logger.trace("Re-entrant call detected! Saving {} bytes.", chunk.remaining());
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

        // --- PRE-FLIGHT LENGTH CHECK FOR STATIC TOKENS ---
        if (expectingNewToken) {
          byte peekType = accumulator.get(accumulator.position());
          TokenType typeEnum = TokenType.fromValue(peekType);

          if (typeEnum != null) {
            int fixedLen = typeEnum.getFixedPayloadLength();
            // Check if we have enough bytes for the token type byte (1) + its fixed payload
            if (fixedLen > 0 && accumulator.remaining() < (1 + fixedLen)) {
              logger.trace("Pre-flight check: Waiting for {} more bytes for {} token.", (1 + fixedLen) - accumulator.remaining(), typeEnum);
              break;
            }
          }

          currentTokenType = accumulator.get(); // Consume the type byte
          expectingNewToken = false;
        }

        if (currentTokenType == TokenType.ROW.getValue()) {
          logger.trace("Handing over to processRowToken...");

          // Pass the buffer to the specialized ROW handler
          if (!processRowToken(accumulator, isEom)) {
            logger.trace("processRowToken needed more bytes. Breaking loop.");
            break;
          }

          logger.trace("ROW token emitted. Protocol-Driven TCP Backpressure invoked.");
          transport.suspendNetworkRead();
          expectingNewToken = true;
          break; // Exit loop, wait for worker thread to signal resume
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

          // --- CAPTURE THE METADATA BLUEPRINT ---
          if (token instanceof ColMetaDataToken) {
            this.currentMetaData = (ColMetaDataToken) token;
            logger.trace("StatefulTokenDecoder captured new ColMetaDataToken for ROW boundary scanning.");
          }

          visitor.onToken(token);
          expectingNewToken = true;
        } catch (BufferUnderflowException e) {
          accumulator.reset();
          break;
        } catch (Exception e) {
          logger.error("Error parsing token type 0x{:02X}", currentTokenType, e);
          visitor.onError(e);
          return;
        }
      }
      accumulator.compact();
      logger.trace("Loop finished. Accumulator compacted. Remaining bytes: {}", accumulator.position());
    } finally {
      isParsing.set(false);
      if (pendingReentrantBytes != null) {
        logger.trace("Processing {} pending re-entrant bytes after main loop.", pendingReentrantBytes.position());
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

      // 1. Proactively scan the boundaries using the saved metadata
      int rowPayloadLength = RowBoundaryScanner.calculateRowLength(buffer, currentMetaData);

      // 2. Create a perfectly-sized slice to isolate the worker thread
      int originalLimit = buffer.limit();
      buffer.limit(buffer.position() + rowPayloadLength);
      ByteBuffer rowSlice = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
      buffer.limit(originalLimit); // Restore the limit for the network thread

      // 3. Manually advance the network buffer past the row we just sliced
      buffer.position(buffer.position() + rowPayloadLength);

      // 4. Pass the strictly-bounded slice to the deferred parser
      TokenParser parser = registry.getParser(TokenType.ROW.getValue());
      Token token = parser.parse(rowSlice, currentTokenType, context);

      visitor.onToken(token);
      return true;

    } catch (BufferUnderflowException e) {
      logger.debug("ROW scanner underflowed during pre-flight check! Waiting for next network chunk.");
      buffer.reset();
      return false;
    } catch (Exception e) {
      logger.error("STATEFUL: Error parsing ROW token boundaries", e);
      throw e;
    }
  }
}