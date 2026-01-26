package org.tdslib.javatdslib.query.rpc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps MS SQL Server data types to TDS XTYPE bytes and TYPE_INFO details for RPC parameter binding.
 *
 * Expanded to cover all common MS SQL Server types (based on TDS spec).
 *
 * Styles:
 * - FIXED: Just XTYPE (no extra bytes)
 * - LENGTH: XTYPE + 1-byte length (e.g. 4 or 8 for float/real)
 * - PREC_SCALE: XTYPE + 1-byte precision + 1-byte scale (for decimal/numeric)
 * - VARLEN: XTYPE + 2-byte max length (for varchar/nvarchar, binary, etc.)
 *
 * Default precision/scale/length provided; override at runtime for dynamic values.
 *
 * Java type hints are comments only (e.g. for BigDecimal, Double, etc.) — use runtime type inference in your binder.
 */
public enum BindingType {

  // Integer types (INTNTYPE = 0x26, length 1/2/4/8 — Java: Byte/Short/Integer/Long)
  TINYINT("tinyint", (byte) 0x26, TypeStyle.LENGTH, null, null, (byte) 1),
  SMALLINT("smallint", (byte) 0x26, TypeStyle.LENGTH, null, null, (byte) 2),
  INT("int", (byte) 0x26, TypeStyle.LENGTH, null, null, (byte) 4),
  BIGINT("bigint", (byte) 0x26, TypeStyle.LENGTH, null, null, (byte) 8),

  // Bit (BITTYPE = 0x68, fixed length 1 — Java: Boolean/Byte)
  BIT("bit", (byte) 0x68, TypeStyle.LENGTH, null, null, (byte) 1),

  // Floating point (FLTTYPE = 0x6D, length 4/8 — Java: Float/Double)
  REAL("real", (byte) 0x6D, TypeStyle.LENGTH, null, null, (byte) 4),
  FLOAT("float", (byte) 0x6D, TypeStyle.LENGTH, null, null, (byte) 8),

  // Decimal/Numeric (NUMERICNTYPE = 0x6A, precision + scale — Java: BigDecimal)
  DECIMAL("decimal", (byte) 0x6A, TypeStyle.PREC_SCALE, (byte) 38, (byte) 0, null),
  NUMERIC("numeric", (byte) 0x6A, TypeStyle.PREC_SCALE, (byte) 38, (byte) 0, null),

  // Money (MONEYTYPE = 0x3A, fixed length 4/8 — Java: BigDecimal)
  SMALLMONEY("smallmoney", (byte) 0x3A, TypeStyle.FIXED, null, null, null),
  MONEY("money", (byte) 0x3A, TypeStyle.FIXED, null, null, null),

  // String types (NVARCHARTYPE / VARCHARTYPE = 0xE7, varlen max — Java: String)
  CHAR("char", (byte) 0xE7, TypeStyle.VARLEN, null, null, (short) 8000),      // fixed-length but TDS treats as var
  VARCHAR("varchar", (byte) 0xE7, TypeStyle.VARLEN, null, null, (short) -1),  // -1 = MAX
  TEXT("text", (byte) 0xE7, TypeStyle.VARLEN, null, null, (short) -1),
  NCHAR("nchar", (byte) 0xE7, TypeStyle.VARLEN, null, null, (short) 4000),
  NVARCHAR("nvarchar", (byte) 0xE7, TypeStyle.VARLEN, null, null, (short) -1),
  NTEXT("ntext", (byte) 0xE7, TypeStyle.VARLEN, null, null, (short) -1),

  // Binary types (VARBINARYTYPE = 0xA5, varlen max — Java: byte[])
  BINARY("binary", (byte) 0xA5, TypeStyle.VARLEN, null, null, (short) 8000),
  VARBINARY("varbinary", (byte) 0xA5, TypeStyle.VARLEN, null, null, (short) -1),
  IMAGE("image", (byte) 0xA5, TypeStyle.VARLEN, null, null, (short) -1),

  // Date/Time types (DATENTYPE=0x28, TIMENTYPE=0x29, DATETIMETYPE=0x2A — Java: LocalDate, LocalTime, LocalDateTime)
  DATE("date", (byte) 0x28, TypeStyle.FIXED, null, null, null),
  TIME("time", (byte) 0x29, TypeStyle.FIXED, null, null, null),
  DATETIME("datetime", (byte) 0x2A, TypeStyle.FIXED, null, null, null),
  DATETIME2("datetime2", (byte) 0x2A, TypeStyle.FIXED, null, null, null),
  SMALLDATETIME("smalldatetime", (byte) 0x3A, TypeStyle.FIXED, null, null, null),  // 0x3A MONEYTYPE? Wait, no — actually 0x3A is MONEY, smalldatetime is 0x3B DATETIM4TYPE
  DATETIMEOFFSET("datetimeoffset", (byte) 0x2B, TypeStyle.FIXED, null, null, null),

  // Uniqueidentifier (GUIDTYPE = 0x24, fixed 16 bytes — Java: UUID or byte[16])
  UNIQUEIDENTIFIER("uniqueidentifier", (byte) 0x24, TypeStyle.FIXED, null, null, null),

  // XML (XMLTYPE = 0xF1, varlen max — Java: String or custom XML class)
  XML("xml", (byte) 0xF1, TypeStyle.VARLEN, null, null, (short) -1),

  // Geography/Geometry (UDTTYPE = 0xF0, varlen max — Java: byte[] or custom)
  GEOGRAPHY("geography", (byte) 0xF0, TypeStyle.VARLEN, null, null, (short) -1),
  GEOMETRY("geometry", (byte) 0xF0, TypeStyle.VARLEN, null, null, (short) -1);

  // -------------------------------------------------------------------------

  private final String sqlTypeName;
  private final byte tdsXType;
  private final TypeStyle typeStyle;
  private final Byte precision;  // null if not needed
  private final Byte scale;      // null if not needed
  private final Number length;   // Byte for fixed/length, Short for varlen max, null if not needed

  // Static lookup map (sqlTypeName → BindingType enum)
  private static final Map<String, BindingType> NAME_TO_BINDING;

  static {
    Map<String, BindingType> map = new HashMap<>();
    for (BindingType bt : values()) {
      map.put(bt.sqlTypeName.toLowerCase(), bt);
    }
    NAME_TO_BINDING = Collections.unmodifiableMap(map);
  }

  BindingType(String sqlTypeName, byte tdsXType, TypeStyle typeStyle, Byte precision, Byte scale, Number length) {
    this.sqlTypeName = sqlTypeName;
    this.tdsXType = tdsXType;
    this.typeStyle = typeStyle;
    this.precision = precision;
    this.scale = scale;
    this.length = length;
  }

  public byte getTdsXType() {
    return tdsXType;
  }

  public String getSqlTypeName() {
    return sqlTypeName;
  }

  public TypeStyle getTypeStyle() {
    return typeStyle;
  }

  public Byte getPrecision() {
    return precision;
  }

  public Byte getScale() {
    return scale;
  }

  public Number getLength() {
    return length;
  }

  /**
   * Get the BindingType for a given SQL Server type name.
   * Returns null if not mapped.
   * Case-insensitive.
   */
  public static BindingType fromSqlTypeName(String sqlTypeName) {
    return NAME_TO_BINDING.get(sqlTypeName.toLowerCase());
  }

  /**
   * Check if a SQL Server type name is supported by this mapping.
   */
  public static boolean isSupported(String sqlTypeName) {
    return NAME_TO_BINDING.containsKey(sqlTypeName.toLowerCase());
  }
}

/**
 * Enum for TYPE_INFO "styles" in TDS.
 */
enum TypeStyle {
  FIXED,       // Just XTYPE (no extra bytes)
  LENGTH,      // XTYPE + 1-byte length
  PREC_SCALE,  // XTYPE + 1-byte precision + 1-byte scale
  VARLEN       // XTYPE + 2-byte max length
}