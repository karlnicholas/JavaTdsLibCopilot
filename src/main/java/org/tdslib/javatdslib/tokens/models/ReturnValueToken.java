package org.tdslib.javatdslib.tokens.models;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Represents a RETURNVALUE token in the TDS protocol. This token is used to return output
 * parameters from stored procedures or user-defined functions.
 */
public final class ReturnValueToken extends Token {

  private final int ordinal;
  private final String paramName;
  private final byte statusFlags;
  private final TypeInfo typeInfo; // Added
  private final byte[] value;

  /**
   * Constructs a new ReturnValueToken.
   *
   * @param tokenType The byte value representing the token type.
   * @param ordinal The ordinal position of the parameter.
   * @param paramName The name of the parameter.
   * @param statusFlags The status flags associated with the parameter.
   * @param typeInfo The type information for the parameter.
   * @param value The raw byte value of the parameter.
   */
  public ReturnValueToken(
      final byte tokenType,
      final int ordinal,
      final String paramName,
      final byte statusFlags,
      final TypeInfo typeInfo,
      final byte[] value) {
    super(TokenType.fromValue(tokenType));
    this.ordinal = ordinal;
    this.paramName = paramName != null ? paramName : "";
    this.statusFlags = statusFlags;
    this.typeInfo = typeInfo;
    this.value = value;
  }

  public int getOrdinal() {
    return ordinal;
  }

  public String getParamName() {
    return paramName;
  }

  public byte getStatusFlags() {
    return statusFlags;
  }

  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

  public byte[] getValue() {
    return value;
  }

  public boolean isOutputParameter() {
    return (statusFlags & 0x01) != 0;
  }
}
