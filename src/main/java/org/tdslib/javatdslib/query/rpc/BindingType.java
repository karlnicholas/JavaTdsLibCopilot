package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.TdsDataType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps MS SQL Server data types to TDS XTYPE bytes and TYPE_INFO details for RPC parameter binding.
 *
 * Uses constants from {@link TdsDataType}.
 *
 * Styles:
 * - FIXED: Just XTYPE (no extra bytes)
 * - LENGTH: XTYPE + 1-byte length (e.g. 4 or 8 for float/real)
 * - PREC_SCALE: XTYPE + 1-byte precision + 1-byte scale (for decimal/numeric)
 * - VARLEN: XTYPE + 2-byte max length (for varchar/nvarchar, binary, etc.)
 */
public enum BindingType {
  // Integer types (INTN = 0x26 - handles tiny, small, int, big via length 1/2/4/8)
  TINYINT("tinyint", (byte) TdsDataType.INTN, TypeStyle.LENGTH, null, null, (byte) 1),
  SMALLINT("smallint", (byte) TdsDataType.INTN, TypeStyle.LENGTH, null, null, (byte) 2),
  INT("int", (byte) TdsDataType.INTN, TypeStyle.LENGTH, null, null, (byte) 4),
  BIGINT("bigint", (byte) TdsDataType.INTN, TypeStyle.LENGTH, null, null, (byte) 8),

  // Bit (BITN = 0x68 - Nullable Bit)
  BIT("bit", (byte) TdsDataType.BITN, TypeStyle.LENGTH, null, null, (byte) 1),

  // Floating point (FLTN = 0x6D - handles Real/Float via length 4/8)
  REAL("real", (byte) TdsDataType.FLTN, TypeStyle.LENGTH, null, null, (byte) 4),
  FLOAT("float", (byte) TdsDataType.FLTN, TypeStyle.LENGTH, null, null, (byte) 8),

  // Decimal/Numeric (DECIMALN = 0x6A, NUMERICN = 0x6C)
  DECIMAL("decimal", (byte) TdsDataType.DECIMALN, TypeStyle.PREC_SCALE, (byte) 38, (byte) 0, null),
  NUMERIC("numeric", (byte) TdsDataType.NUMERICN, TypeStyle.PREC_SCALE, (byte) 38, (byte) 0, null),

  // Money (MONEYN = 0x6E - handles SmallMoney/Money via length 4/8)
  SMALLMONEY("smallmoney", (byte) TdsDataType.MONEYN, TypeStyle.LENGTH, null, null, (byte) 4),
  MONEY("money", (byte) TdsDataType.MONEYN, TypeStyle.LENGTH, null, null, (byte) 8),

  // String types
  // Note: Using NVARCHAR (0xE7) for CHAR is a common strategy to ensure Unicode compatibility,
  // but standard CHAR is BIGVARCHR (0xA7) or CHAR (0x2F).
  // Below mappings preserve your logic of preferring Unicode (NVARCHAR) for most string types.
  CHAR("char", (byte) TdsDataType.NVARCHAR, TypeStyle.VARLEN, null, null, (short) 8000),
  VARCHAR("varchar(max)", (byte) TdsDataType.BIGVARCHR, TypeStyle.VARLEN, null, null, (short) -1),
  TEXT("text", (byte) TdsDataType.BIGVARCHR, TypeStyle.VARLEN, null, null, (short) -1),

  NCHAR("nchar", (byte) TdsDataType.NVARCHAR, TypeStyle.VARLEN, null, null, (short) 4000),
  NVARCHAR("nvarchar (max)", (byte) TdsDataType.NVARCHAR, TypeStyle.VARLEN, null, null, (short) -1),
  NTEXT("ntext", (byte) TdsDataType.NVARCHAR, TypeStyle.VARLEN, null, null, (short) -1),

  // Binary types (BIGVARBIN = 0xA5)
  BINARY("binary", (byte) TdsDataType.BIGVARBIN, TypeStyle.VARLEN, null, null, (short) 8000),
  VARBINARY("varbinary(max)", (byte) TdsDataType.BIGVARBIN, TypeStyle.VARLEN, null, null, (short) -1),
  IMAGE("image", (byte) TdsDataType.BIGVARBIN, TypeStyle.VARLEN, null, null, (short) -1),

  // Date/Time types
  DATE("date", (byte) TdsDataType.DATE, TypeStyle.FIXED, null, null, null),
  TIME("time", (byte) TdsDataType.TIME, TypeStyle.FIXED, null, null, null),

  // Using DATETIMN (0x6F) allows for nullable DateTime/SmallDateTime
  DATETIME("datetime", (byte) TdsDataType.DATETIMN, TypeStyle.LENGTH, null, null, (byte) 8),
  SMALLDATETIME("smalldatetime", (byte) TdsDataType.DATETIMN, TypeStyle.LENGTH, null, null, (byte) 4),

  DATETIME2("datetime2(7)", (byte) TdsDataType.DATETIME2, TypeStyle.FIXED, null, null, null),
  DATETIMEOFFSET("datetimeoffset", (byte) TdsDataType.DATETIMEOFFSET, TypeStyle.FIXED, null, null, null),

  // Uniqueidentifier
  UNIQUEIDENTIFIER("uniqueidentifier", (byte) TdsDataType.GUID, TypeStyle.FIXED, null, null, null),

  // XML
  XML("xml", (byte) TdsDataType.XML, TypeStyle.VARLEN, null, null, (short) -1),

  // UDT (Geography/Geometry)
  GEOGRAPHY("geography", (byte) TdsDataType.UDT, TypeStyle.VARLEN, null, null, (short) -1),
  GEOMETRY("geometry", (byte) TdsDataType.UDT, TypeStyle.VARLEN, null, null, (short) -1);

  // -------------------------------------------------------------------------

  private final String sqlTypeName;
  private final byte tdsXType;
  private final TypeStyle typeStyle;
  private final Byte precision;
  private final Byte scale;
  private final Number length;

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

  public static BindingType fromSqlTypeName(String sqlTypeName) {
    return NAME_TO_BINDING.get(sqlTypeName.toLowerCase());
  }

  public static boolean isSupported(String sqlTypeName) {
    return NAME_TO_BINDING.containsKey(sqlTypeName.toLowerCase());
  }
}