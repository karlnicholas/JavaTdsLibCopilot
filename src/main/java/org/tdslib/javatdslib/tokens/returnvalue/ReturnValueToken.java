package org.tdslib.javatdslib.tokens.returnvalue;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Return value token (0xAC) - contains an output parameter value or procedure return value
 * from an RPC/stored procedure call.
 */
public final class ReturnValueToken extends Token {

  private final String paramName;     // may be empty
  private final byte statusFlags;     // e.g. 0x01 = output parameter
  private final Object value;         // the actual returned value (boxed)

  /**
   * Constructs a ReturnValueToken.
   *
   * @param tokenType  raw token type byte (should be 0xAC)
   * @param paramName  parameter name (can be empty string)
   * @param statusFlags status byte from the token stream
   * @param value      the decoded value (Integer, String, etc.)
   */
  public ReturnValueToken(final byte tokenType,
                          final String paramName,
                          final byte statusFlags,
                          final Object value) {
    super(TokenType.fromValue(tokenType));
    this.paramName = paramName != null ? paramName : "";
    this.statusFlags = statusFlags;
    this.value = value;
  }

  public String getParamName() {
    return paramName;
  }

  public byte getStatusFlags() {
    return statusFlags;
  }

  public Object getValue() {
    return value;
  }

  public boolean isOutputParameter() {
    return (statusFlags & 0x01) != 0;
  }

  @Override
  public String toString() {
    return String.format("ReturnValueToken{param=%s, flags=0x%02X, value=%s}",
        paramName.isEmpty() ? "<return>" : paramName,
        statusFlags,
        value);
  }
}