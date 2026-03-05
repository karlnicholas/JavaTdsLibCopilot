package org.tdslib.javatdslib.protocol;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of TDS data types. This enum maps TDS type identifiers to their corresponding
 * length strategies and fixed sizes, and provides utility methods for type inference.
 */
public enum TdsType {

  // --- Integers ---
  INT1(0x30, LengthStrategy.FIXED, 1),
  INT2(0x34, LengthStrategy.FIXED, 2),
  INT4(0x38, LengthStrategy.FIXED, 4),
  INT8(0x7F, LengthStrategy.FIXED, 8),
  INTN(0x26, LengthStrategy.BYTELEN, 8),

  // --- Floats ---
  FLT4(0x3B, LengthStrategy.FIXED, 4),
  REAL(0x3B, LengthStrategy.FIXED, 4),
  FLT8(0x3E, LengthStrategy.FIXED, 8),
  FLTN(0x6D, LengthStrategy.BYTELEN, 8),

  // --- Bit ---
  BIT(0x32, LengthStrategy.FIXED, 1),
  BITN(0x68, LengthStrategy.BYTELEN, 1),

  // --- Decimals ---
  NUMERIC(0x3F, LengthStrategy.PREC_SCALE, 17),
  DECIMAL(0x37, LengthStrategy.PREC_SCALE, 17),
  NUMERICN(0x6C, LengthStrategy.BYTELEN, 17),
  DECIMALN(0x6A, LengthStrategy.BYTELEN, 17),
  MONEYN(0x6E, LengthStrategy.BYTELEN, 8),
  MONEY(0x3C, LengthStrategy.FIXED, 8),
  SMALLMONEY(0x7A, LengthStrategy.FIXED, 4),

  // --- Dates ---
  DATE(0x28, LengthStrategy.FIXED, 3),
  TIME(0x29, LengthStrategy.SCALE_LEN, 5),
  DATETIME2(0x2A, LengthStrategy.SCALE_LEN, 8),
  DATETIMEOFFSET(0x2B, LengthStrategy.SCALE_LEN, 10),
  DATETIMN(0x6F, LengthStrategy.BYTELEN, 8),
  DATETIME(0x3D, LengthStrategy.FIXED, 8),
  SMALLDATETIME(0x3A, LengthStrategy.FIXED, 4),

  // --- Strings & Binary ---
  BIGVARCHR(0xA7, LengthStrategy.USHORTLEN, -1),
  BIGCHAR(0xAF, LengthStrategy.USHORTLEN, -1),
  BIGBINARY(0xAD, LengthStrategy.USHORTLEN, -1),
  NVARCHAR(0xE7, LengthStrategy.USHORTLEN, -1),
  BIGVARBIN(0xA5, LengthStrategy.USHORTLEN, -1),
  NCHAR(0xEF, LengthStrategy.USHORTLEN, -1),

  // Legacy Types (Avoid using these in RPC)
  CHAR(0x2F, LengthStrategy.USHORTLEN, -1),
  VARCHAR(0x27, LengthStrategy.USHORTLEN, -1),
  BINARY(0x2D, LengthStrategy.USHORTLEN, -1),
  VARBINARY(0x25, LengthStrategy.USHORTLEN, -1),

  // --- Large Objects ---
  XML(0xF1, LengthStrategy.PLP, -1),
  TEXT(0x23, LengthStrategy.LONGLEN, -1),
  NTEXT(0x63, LengthStrategy.LONGLEN, -1),
  IMAGE(0x22, LengthStrategy.LONGLEN, -1),

  // --- GUID ---
  GUID(0x24, LengthStrategy.BYTELEN, 16);

  public final int byteVal;
  public final LengthStrategy strategy;
  public final int fixedSize;

  TdsType(int byteVal, LengthStrategy strategy, int fixedSize) {
    this.byteVal = byteVal;
    this.strategy = strategy;
    this.fixedSize = fixedSize;
  }

  private static final Map<Integer, TdsType> BYTE_MAP = new HashMap<>();

  static {
    for (TdsType t : values()) {
      BYTE_MAP.put(t.byteVal, t);
    }
  }

  public static TdsType valueOf(byte b) {
    return BYTE_MAP.getOrDefault(b & 0xFF, null);
  }

  /**
   * Strategy for determining the length of a TDS type.
   */
  public enum LengthStrategy {
    FIXED,
    BYTELEN,
    USHORTLEN,
    SCALE_LEN,
    PLP,
    PREC_SCALE,
    LONGLEN
  }

  /**
   * Infers the TDS type from a Java class.
   *
   * @param clazz The Java class to infer the TDS type from.
   * @return The inferred TDS type, or null if no mapping exists.
   */
  public static TdsType inferFromJavaType(Class<?> clazz) {
    if (clazz == Integer.class || clazz == int.class) {
      return INTN;
    }
    if (clazz == Long.class || clazz == long.class) {
      return INTN;
    }
    if (clazz == Short.class || clazz == short.class) {
      return INTN;
    }
    if (clazz == Byte.class || clazz == byte.class) {
      return INTN;
    }
    if (java.math.BigDecimal.class.isAssignableFrom(clazz)) {
      return DECIMALN;
    }
    if (clazz == Double.class || clazz == double.class) {
      return FLTN;
    }
    if (clazz == Float.class || clazz == float.class) {
      return REAL;
    }
    if (clazz == Boolean.class || clazz == boolean.class) {
      return BITN;
    }
    if (clazz == java.time.LocalDate.class) {
      return DATE;
    }
    if (clazz == java.time.LocalTime.class) {
      return TIME;
    }
    if (clazz == java.time.LocalDateTime.class) {
      return DATETIME2;
    }
    if (clazz == java.time.OffsetDateTime.class) {
      return DATETIMEOFFSET;
    }
    if (clazz == String.class) {
      return NVARCHAR;
    }
    if (java.nio.ByteBuffer.class.isAssignableFrom(clazz) || clazz == byte[].class) {
      return BIGVARBIN;
    }
    if (clazz == java.util.UUID.class) {
      return GUID;
    }
    return null;
  }
}
