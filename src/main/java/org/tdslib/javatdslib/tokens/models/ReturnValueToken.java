package org.tdslib.javatdslib.tokens.models;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

public final class ReturnValueToken extends Token {

  private final int ordinal;
  private final String paramName;
  private final byte statusFlags;
  private final TypeInfo typeInfo; // Added
  private final byte[] value;

  public ReturnValueToken(final byte tokenType,
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

  public int getOrdinal() { return ordinal; }
  public String getParamName() { return paramName; }
  public byte getStatusFlags() { return statusFlags; }
  public TypeInfo getTypeInfo() { return typeInfo; }
  public byte[] getValue() { return value; }

  public boolean isOutputParameter() {
    return (statusFlags & 0x01) != 0;
  }
}