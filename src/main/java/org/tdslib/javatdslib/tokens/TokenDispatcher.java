package org.tdslib.javatdslib.tokens;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Dispatches parsing of individual TDS tokens from a single message payload.
 * Processes one TdsMessage (one packet) at a time — no stream across packets.
 */
public class TokenDispatcher {
  private static final Logger logger = LoggerFactory.getLogger(TokenDispatcher.class);

  private final TokenParserRegistry registry;

  public TokenDispatcher(TokenParserRegistry registry) {
    this.registry = registry;
  }

  /**
   * Processes all tokens in a single TDS tdsMessage (one packet's payload).
   * Calls the visitor for each successfully parsed token.
   *
   * @param tdsMessage The TDS packet/tdsMessage to parse
   * @param connectionContext Connection context for state updates
   * @param visitor Callback to handle each parsed token
   */
  public void processMessage(final TdsMessage tdsMessage, final ConnectionContext connectionContext,
                             final TokenVisitor visitor) {
    final ByteBuffer payload = tdsMessage.getPayload();
    payload.order(ByteOrder.LITTLE_ENDIAN);

    while (payload.hasRemaining()) {
      final byte tokenTypeByte = payload.get();

      final TokenParser parser = registry.getParser(tokenTypeByte);
      if (parser == null) {
        final String err = "No parser registered for token type 0x"
            + Integer.toHexString(tokenTypeByte & 0xFF);
        throw new IllegalStateException(err);
      }

      logger.trace("Parsing token type " + TokenType.fromValue(tokenTypeByte).name());

      final Token token = parser.parse(payload, tokenTypeByte, connectionContext);
      visitor.onToken(token);
    }

    if (tdsMessage.isResetConnection()) {
      connectionContext.resetToDefaults();
    }
  }
}