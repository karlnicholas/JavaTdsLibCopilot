package org.tdslib.javatdslib.tokens;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsStreamHandler;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A stateful decoder that processes raw byte chunks from the network,
 * handles TCP fragmentation, and emits complete Tokens to the visitor chain.
 */
public class StatefulTokenDecoder implements TdsStreamHandler {
  private static final Logger logger = LoggerFactory.getLogger(StatefulTokenDecoder.class);

  private final TokenParserRegistry registry;
  private final ConnectionContext context;
  private final TokenVisitor visitor;

  // FIX: Force LITTLE_ENDIAN for MS-TDS payload parsing
  private ByteBuffer accumulator = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);

  // State tracking
  private boolean expectingNewToken = true;
  private byte currentTokenType = 0;

  public StatefulTokenDecoder(TokenParserRegistry registry, ConnectionContext context, TokenVisitor visitor) {
    this.registry = registry;
    this.context = context;
    this.visitor = visitor;
  }

  @Override
  public void onPayloadAvailable(ByteBuffer chunk, boolean isEom) {
    // 1. Add incoming chunk to our accumulator
    if (accumulator.remaining() < chunk.remaining()) {
      // FIX: Ensure expanded buffer is also LITTLE_ENDIAN
      ByteBuffer newAcc = ByteBuffer.allocate(accumulator.capacity() + chunk.remaining() + 4096)
          .order(ByteOrder.LITTLE_ENDIAN);
      accumulator.flip();
      newAcc.put(accumulator);
      accumulator = newAcc;
    }
    accumulator.put(chunk);

    // Prepare accumulator for reading
    accumulator.flip();

    // 2. Parse as many tokens as possible from the accumulated bytes
    while (accumulator.hasRemaining()) {
      if (expectingNewToken) {
        currentTokenType = accumulator.get();
        expectingNewToken = false;
        logger.trace("STATEFUL: Starting to parse new token type: 0x{}", String.format("%02X", currentTokenType));
      }

      if (currentTokenType == TokenType.ROW.getValue()) {
        if (!processRowToken(accumulator)) {
          break; // Not enough bytes to finish the row, wait for next chunk
        }
        expectingNewToken = true;
        continue;
      }

      accumulator.mark(); // Mark position before we attempt to parse
      TokenParser parser = registry.getParser(currentTokenType);

      if (parser == null) {
        logger.warn("No parser found for token type: 0x{}", String.format("%02X", currentTokenType));
        expectingNewToken = true;
        continue;
      }

      try {
        Token token = parser.parse(accumulator, currentTokenType, context);
        visitor.onToken(token);
        expectingNewToken = true;

      } catch (java.nio.BufferUnderflowException e) {
        // Genuine fragmentation. Wait for more network bytes.
        logger.trace("STATEFUL: Not enough bytes to finish token 0x{}. Buffering...", String.format("%02X", currentTokenType));
        accumulator.reset();
        break;
      } catch (Exception e) {
        logger.error("STATEFUL: Unexpected error parsing token 0x{}", String.format("%02X", currentTokenType), e);
        throw e;
      }
    }

    // 3. Compact the accumulator to save any unread bytes for the next network chunk
    accumulator.compact();
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