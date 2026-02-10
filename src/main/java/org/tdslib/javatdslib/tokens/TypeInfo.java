package org.tdslib.javatdslib.tokens;

import org.tdslib.javatdslib.TdsType;

/**
 * Encapsulates the TYPE_INFO structure defined in the TDS specification.
 * Shared by COLMETADATA, RETURNVALUE, and ALTMETADATA tokens.
 */
public class TypeInfo {
  private final TdsType tdsType;
  private final int maxLength;
  private final byte precision;
  private final byte scale;
  private final byte[] collation;

  public TypeInfo(TdsType tdsType, int maxLength, byte precision, byte scale, byte[] collation) {
    this.tdsType = tdsType;
    this.maxLength = maxLength;
    this.precision = precision;
    this.scale = scale;
    this.collation = collation;
  }

  public TdsType getTdsType() {
    return tdsType;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public byte getPrecision() {
    return precision;
  }

  public byte getScale() {
    return scale;
  }

  public byte[] getCollation() {
    return collation;
  }
}