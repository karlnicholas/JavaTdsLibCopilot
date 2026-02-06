package org.tdslib.javatdslib;

import io.r2dbc.spi.R2dbcType;
import java.util.HashMap;
import java.util.Map;

// NO IMPORTS from java.sql.*
// NO IMPORTS from TdsDataType

public enum TdsType {

  // --- Integers ---
  INT1(0x30, R2dbcType.TINYINT, LengthStrategy.FIXED, 1),
  INT2(0x34, R2dbcType.SMALLINT, LengthStrategy.FIXED, 2),
  INT4(0x38, R2dbcType.INTEGER, LengthStrategy.FIXED, 4),
  INT8(0x7F, R2dbcType.BIGINT, LengthStrategy.FIXED, 8),
  INTN(0x26, R2dbcType.INTEGER, LengthStrategy.BYTELEN, 8),

  // --- Floats ---
  FLT4(0x3B, R2dbcType.REAL, LengthStrategy.FIXED, 4),
  FLT8(0x3E, R2dbcType.DOUBLE, LengthStrategy.FIXED, 8),
  FLTN(0x6D, R2dbcType.DOUBLE, LengthStrategy.BYTELEN, 8),

  // --- Bit ---
  BIT(0x32, R2dbcType.BOOLEAN, LengthStrategy.FIXED, 1),
  BITN(0x68, R2dbcType.BOOLEAN, LengthStrategy.BYTELEN, 1),

  // --- Decimals ---
  NUMERIC(0x3F, R2dbcType.NUMERIC, LengthStrategy.PREC_SCALE, 17),
  DECIMAL(0x37, R2dbcType.DECIMAL, LengthStrategy.PREC_SCALE, 17),
  NUMERICN(0x6C, R2dbcType.NUMERIC, LengthStrategy.BYTELEN, 17),
  DECIMALN(0x37, R2dbcType.DECIMAL, LengthStrategy.BYTELEN, 17), // Note: 0x37 reused
  MONEYN(0x7A, R2dbcType.DECIMAL, LengthStrategy.BYTELEN, 8),
  MONEY(0x3C, R2dbcType.DECIMAL, LengthStrategy.FIXED, 8),
  SMALLMONEY(0x7A, R2dbcType.DECIMAL, LengthStrategy.FIXED, 4),

  // --- Dates ---
  DATE(0x28, R2dbcType.DATE, LengthStrategy.FIXED, 3),
  TIME(0x29, R2dbcType.TIME, LengthStrategy.SCALE_LEN, 5),
  DATETIME2(0x2A, R2dbcType.TIMESTAMP, LengthStrategy.SCALE_LEN, 8),
  DATETIMEOFFSET(0x2B, R2dbcType.TIMESTAMP_WITH_TIME_ZONE, LengthStrategy.SCALE_LEN, 10),
  DATETIMN(0x6F, R2dbcType.TIMESTAMP, LengthStrategy.BYTELEN, 8),
  DATETIME(0x3D, R2dbcType.TIMESTAMP, LengthStrategy.FIXED, 8),
  SMALLDATETIME(0x3A, R2dbcType.TIMESTAMP, LengthStrategy.FIXED, 4),

  // --- Strings & Binary ---
  BIGVARCHR(0xA7, R2dbcType.VARCHAR, LengthStrategy.USHORTLEN, -1), // Modern VARCHAR(MAX)
  NVARCHAR(0xE7, R2dbcType.NVARCHAR, LengthStrategy.USHORTLEN, -1),
  BIGVARBIN(0xA5, R2dbcType.VARBINARY, LengthStrategy.USHORTLEN, -1),

  // Legacy String/Binary (Added to fix switch case errors)
  CHAR(0x2F, R2dbcType.CHAR, LengthStrategy.FIXED, -1),
  VARCHAR(0x27, R2dbcType.VARCHAR, LengthStrategy.FIXED, -1),   // Legacy VARCHAR
  NCHAR(0xEF, R2dbcType.NCHAR, LengthStrategy.USHORTLEN, -1),
  BINARY(0x2D, R2dbcType.BINARY, LengthStrategy.FIXED, -1),
  VARBINARY(0x25, R2dbcType.VARBINARY, LengthStrategy.FIXED, -1), // Legacy VARBINARY

  // --- Large Objects ---
  XML(0xF1, R2dbcType.NVARCHAR, LengthStrategy.PLP, -1),
  TEXT(0x23, R2dbcType.VARCHAR, LengthStrategy.FIXED, -1),
  NTEXT(0x63, R2dbcType.NVARCHAR, LengthStrategy.FIXED, -1),
  IMAGE(0x22, R2dbcType.VARBINARY, LengthStrategy.FIXED, -1),

  // --- GUID ---
  GUID(0x24, R2dbcType.CHAR, LengthStrategy.FIXED, 16);

  // --- Implementation ---
  public final int byteVal;
  public final R2dbcType r2dbcType;
  public final LengthStrategy strategy;
  public final int fixedSize;

  TdsType(int byteVal, R2dbcType r2dbcType, LengthStrategy strategy, int fixedSize) {
    this.byteVal = byteVal;
    this.r2dbcType = r2dbcType;
    this.strategy = strategy;
    this.fixedSize = fixedSize;
  }

  private static final Map<Integer, TdsType> BYTE_MAP = new HashMap<>();
  private static final Map<R2dbcType, TdsType> R2DBC_MAP = new HashMap<>();

  static {
    for (TdsType t : values()) {
      BYTE_MAP.put(t.byteVal, t);
      if (t == INTN || t == FLTN || t == BITN || t == NVARCHAR || t == BIGVARCHR || t == BIGVARBIN || t == GUID) {
        R2DBC_MAP.put(t.r2dbcType, t);
      }
    }
    for (R2dbcType r : R2dbcType.values()) R2DBC_MAP.putIfAbsent(r, NVARCHAR);
    R2DBC_MAP.put(R2dbcType.INTEGER, INTN);
    R2DBC_MAP.put(R2dbcType.BOOLEAN, BITN);
  }

  public static TdsType valueOf(byte b) { return BYTE_MAP.getOrDefault(b & 0xFF, null); }
  public static TdsType forR2dbcType(R2dbcType type) { return R2DBC_MAP.get(type); }

  public enum LengthStrategy { FIXED, BYTELEN, USHORTLEN, SCALE_LEN, PLP, PREC_SCALE }

  public static TdsType inferFromJavaType(Class<?> clazz) {
    if (clazz == Integer.class || clazz == int.class) return INTN;
    if (clazz == Long.class || clazz == long.class) return INTN;
    if (clazz == Short.class || clazz == short.class) return INTN;
    if (clazz == Byte.class || clazz == byte.class) return INTN;
    if (java.math.BigDecimal.class.isAssignableFrom(clazz)) return NUMERICN;
    if (clazz == Double.class || clazz == double.class) return FLTN;
    if (clazz == Float.class || clazz == float.class) return FLT4;
    if (clazz == Boolean.class || clazz == boolean.class) return BITN;
    if (clazz == java.time.LocalDate.class) return DATE;
    if (clazz == java.time.LocalTime.class) return TIME;
    if (clazz == java.time.LocalDateTime.class) return DATETIME2;
    if (clazz == java.time.OffsetDateTime.class) return DATETIMEOFFSET;
    if (clazz == String.class) return NVARCHAR;
    if (java.nio.ByteBuffer.class.isAssignableFrom(clazz) || clazz == byte[].class) return BIGVARBIN;
    if (clazz == java.util.UUID.class) return GUID;
    return null;
  }
}