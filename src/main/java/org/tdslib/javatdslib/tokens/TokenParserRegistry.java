package org.tdslib.javatdslib.tokens;

import org.tdslib.javatdslib.tokens.parsers.ColMetaDataTokenParser;
import org.tdslib.javatdslib.tokens.parsers.DoneInProcTokenParser;
import org.tdslib.javatdslib.tokens.parsers.DoneProcTokenParser;
import org.tdslib.javatdslib.tokens.parsers.DoneTokenParser;
import org.tdslib.javatdslib.tokens.parsers.EnvChangeTokenParser;
import org.tdslib.javatdslib.tokens.parsers.FeatureExtAckTokenParser;
import org.tdslib.javatdslib.tokens.parsers.LoginAckTokenParser;
import org.tdslib.javatdslib.tokens.parsers.ReturnStatusTokenParser;
import org.tdslib.javatdslib.tokens.parsers.ReturnValueTokenParser;
import org.tdslib.javatdslib.tokens.parsers.RowTokenParser;

import java.util.HashMap;
import java.util.Map;

/**
 * A central, thread-safe registry for all MS-TDS token parsers.
 */
public class TokenParserRegistry {

  public static final TokenParserRegistry DEFAULT = new TokenParserRegistry();

  static {
    // Register all standard stateless token parsers
    DEFAULT.register(TokenType.ENV_CHANGE, new EnvChangeTokenParser());
    DEFAULT.register(TokenType.LOGIN_ACK, new LoginAckTokenParser());
    DEFAULT.register(TokenType.ERROR, new MessageTokenParser());
    DEFAULT.register(TokenType.INFO, new MessageTokenParser());
    DEFAULT.register(TokenType.DONE, new DoneTokenParser());
    DEFAULT.register(TokenType.FEATURE_EXT_ACK, new FeatureExtAckTokenParser());
    DEFAULT.register(TokenType.COL_METADATA, new ColMetaDataTokenParser());
    DEFAULT.register(TokenType.ROW, new RowTokenParser());
    DEFAULT.register(TokenType.DONE_IN_PROC, new DoneInProcTokenParser());
    DEFAULT.register(TokenType.DONE_PROC, new DoneProcTokenParser());
    DEFAULT.register(TokenType.RETURN_STATUS, new ReturnStatusTokenParser());
    DEFAULT.register(TokenType.RETURN_VALUE, new ReturnValueTokenParser());
  }

  private final Map<Byte, TokenParser> parsers = new HashMap<>();

  public void register(TokenType type, TokenParser parser) {
    parsers.put(type.getValue(), parser);
  }

  public TokenParser getParser(byte tokenTypeByte) {
    return parsers.get(tokenTypeByte);
  }
}