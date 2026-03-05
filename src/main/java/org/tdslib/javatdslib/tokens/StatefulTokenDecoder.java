package org.tdslib.javatdslib.tokens;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * A stateful decoder that processes raw byte chunks from the network, handles TCP fragmentation,
 * and emits complete Tokens to the visitor chain.
 */
public class StatefulTokenDecoder implements TdsStreamHandler {
  private static final Logger logger = LoggerFactory.getLogger(StatefulTokenDecoder.class);

  private final TokenParserRegistry registry;
  private final ConnectionContext context;
  private final TokenVisitor visitor;
  private final TdsTransport transport;

  // FIX: Force LITTLE_ENDIAN for MS-TDS payload parsing
  private ByteBuffer accumulator = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);

  // State tracking
  private boolean expectingNewToken = true;
  private byte currentTokenType = 0;

  // Add these state variables to the top of StatefulTokenDecoder
  private boolean isParsing = false;
  private ByteBuffer pendingReentrantBytes = null;

  /**
   * Constructs a new StatefulTokenDecoder.
   *
   * @param registry The registry of token parsers.
   * @param context The connection context.
   * @param visitor The visitor to receive parsed tokens.
   * @param transport The transport layer for reading data.
   */
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
    // 1. Intercept re-entrant calls to protect the accumulator state
    if (isParsing) {
      if (pendingReentrantBytes == null) {
        pendingReentrantBytes =
            ByteBuffer.allocate(chunk.remaining()).order(ByteOrder.LITTLE_ENDIAN);
      } else if (pendingReentrantBytes.remaining() < chunk.remaining()) {
        ByteBuffer expanded =
            ByteBuffer.allocate(pendingReentrantBytes.capacity() + chunk.remaining() + 1024)
                .order(ByteOrder.LITTLE_ENDIAN);
        pendingReentrantBytes.flip();
        expanded.put(pendingReentrantBytes);
        pendingReentrantBytes = expanded;
      }
      pendingReentrantBytes.put(chunk);
      return;
    }

    isParsing = true;
    try {
      // 2. Add incoming chunk to our accumulator (Your existing logic)
      if (accumulator.remaining() < chunk.remaining()) {
        ByteBuffer newAcc =
            ByteBuffer.allocate(accumulator.capacity() + chunk.remaining() + 4096)
                .order(ByteOrder.LITTLE_ENDIAN);
        accumulator.flip();
        newAcc.put(accumulator);
        accumulator = newAcc;
      }
      accumulator.put(chunk);
      accumulator.flip();

      // 3. Parse tokens (Your existing logic)
      while (accumulator.hasRemaining()) {
        if (expectingNewToken) {
          currentTokenType = accumulator.get();
          expectingNewToken = false;
        }

        if (currentTokenType == TokenType.ROW.getValue()) {
          if (!processRowToken(accumulator)) {
            break;
          }
          expectingNewToken = true;
          continue;
        }

        accumulator.mark();
        TokenParser parser = registry.getParser(currentTokenType);

        if (parser == null) {
          expectingNewToken = true;
          continue;
        }

        try {
          Token token = parser.parse(accumulator, currentTokenType, context);
          visitor.onToken(token);
          expectingNewToken = true;
        } catch (java.nio.BufferUnderflowException e) {
          accumulator.reset();
          break;
        }
      }
      // 4. Compact
      accumulator.compact();

    } finally {
      isParsing = false;
      // 5. If re-entrant data arrived during the loop, safely process it now
      if (pendingReentrantBytes != null) {
        pendingReentrantBytes.flip();
        ByteBuffer toProcess = pendingReentrantBytes;
        pendingReentrantBytes = null;
        onPayloadAvailable(toProcess, isEom);
      }
    }
  }

  private boolean processRowToken(ByteBuffer buffer) {
    try {
      buffer.mark();
      TokenParser parser = registry.getParser(TokenType.ROW.getValue());
      Token token = parser.parse(buffer, currentTokenType, context);
      visitor.onToken(token);
      return true;
    } catch (java.nio.BufferUnderflowException e) {
      buffer.reset();
      return false;
    } catch (Exception e) {
      logger.error("STATEFUL: Error parsing ROW token", e);
      throw e;
    }
  }
}
