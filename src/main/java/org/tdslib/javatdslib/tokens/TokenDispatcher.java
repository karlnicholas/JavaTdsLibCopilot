package org.tdslib.javatdslib.tokens;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.tokens.colmetadata.ColMetaDataTokenParser;
import org.tdslib.javatdslib.tokens.done.DoneInProcTokenParser;
import org.tdslib.javatdslib.tokens.done.DoneProcTokenParser;
import org.tdslib.javatdslib.tokens.done.DoneTokenParser;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeTokenParser;
import org.tdslib.javatdslib.tokens.featureextack.FeatureExtAckTokenParser;
import org.tdslib.javatdslib.tokens.loginack.LoginAckTokenParser;
import org.tdslib.javatdslib.tokens.returnstatus.ReturnStatusTokenParser;
import org.tdslib.javatdslib.tokens.returnvalue.ReturnValueTokenParser;
import org.tdslib.javatdslib.tokens.row.RowTokenParser;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

/**
 * Dispatches parsing of individual TDS tokens from a single message payload.
 * Processes one TdsMessage (one packet) at a time — no stream across packets.
 */
public class TokenDispatcher {
  private final Logger logger = LoggerFactory.getLogger(TokenDispatcher.class);
  private final Map<Byte, TokenParser> parsers = new HashMap<>();

  /**
   * Create and register known token parsers.
   */
  public TokenDispatcher() {
    register(TokenType.ENV_CHANGE, new EnvChangeTokenParser());
    register(TokenType.LOGIN_ACK, new LoginAckTokenParser());
    register(TokenType.ERROR, new MessageTokenParser());
    register(TokenType.INFO, new MessageTokenParser());
    register(TokenType.DONE, new DoneTokenParser());
    register(TokenType.FEATURE_EXT_ACK, new FeatureExtAckTokenParser());

    // Add more parsers as implemented
    register(TokenType.COL_METADATA, new ColMetaDataTokenParser());
    register(TokenType.ROW, new RowTokenParser());
    register(TokenType.DONE_IN_PROC, new DoneInProcTokenParser());
    register(TokenType.DONE_PROC, new DoneProcTokenParser());
    register(TokenType.RETURN_STATUS, new ReturnStatusTokenParser());
    register(TokenType.RETURN_VALUE, new ReturnValueTokenParser());
    // For ROW, you may need to pass last ColMetaDataToken (store in context or
    // use stateful parser)
  }

  /**
   * Registers a parser for the given token type.
   *
   * @param type   the token type to register
   * @param parser the parser instance
   */
  private void register(final TokenType type, final TokenParser parser) {
    parsers.put(type.getValue(), parser);
  }

  /**
   * Processes all tokens in a single TDS tdsMessage (one packet's payload).
   * Calls the visitor for each successfully parsed token.
   *
   * @param tdsMessage The TDS packet/tdsMessage to parse
   * @param connectionContext Connection context for state updates (e.g., packet size,
   *                database)
   * @param visitor Callback to handle each parsed token
   */
  public void processMessage(final TdsMessage tdsMessage, final ConnectionContext connectionContext,
                             final QueryContext queryContext, final TokenVisitor visitor) {
    final ByteBuffer payload = tdsMessage.getPayload();
    payload.order(ByteOrder.LITTLE_ENDIAN);

    while (payload.hasRemaining()) {
      final byte tokenTypeByte = payload.get();

      final TokenParser parser = parsers.get(tokenTypeByte);
      if (parser == null) {
        // Unknown token: throw or skip (throw is safer during dev)
        final String err = "No parser registered for token type 0x"
            + Integer.toHexString(tokenTypeByte & 0xFF);
        throw new IllegalStateException(err);
      }

      logger.trace("Parsing token type " + TokenType.fromValue(tokenTypeByte).name());
      // Parser consumes exactly the bytes for this token
      final Token token = parser.parse(payload, tokenTypeByte, connectionContext, queryContext);
      // Notify the visitor (caller decides what to do)
      visitor.onToken(token,queryContext);
    }

    // Handle resetConnection flag after all tokens in this tdsMessage
    if (tdsMessage.isResetConnection()) {
      connectionContext.resetToDefaults();
    }
  }
}