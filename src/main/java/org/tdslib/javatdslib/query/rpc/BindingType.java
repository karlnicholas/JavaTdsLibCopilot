package org.tdslib.javatdslib.query.rpc;

import java.math.BigDecimal;
import java.sql.*;
import java.time.*; // if adding LocalDate/LocalDateTime later

enum BindingType {
  SHORT(
          Short.class,
          "smallint",
          (byte) 0x34,          // INT2TYPE (fixed)
          (byte) 0x26,          // INTNTYPE (nullable variant)
          "fixed", 2,           // size bytes
          false,                // not PLP
          false,                // no collation
          null                  // no extra bytes config
  ),
  INTEGER(
          Integer.class,
          "int",
          (byte) 0x38,          // INT4TYPE
          (byte) 0x26,          // INTNTYPE
          "fixed", 4,
          false,
          false,
          null
  ),
  LONG(
          Long.class,
          "bigint",
          (byte) 0x7F,          // INT8TYPE
          (byte) 0x26,
          "fixed", 8,
          false,
          false,
          null
  ),
  BYTE(
          Byte.class,
          "tinyint",
          (byte) 0x30,          // INT1TYPE
          (byte) 0x26,
          "fixed", 1,
          false,
          false,
          null
  ),
  BOOLEAN(
          Boolean.class,
          "bit",
          (byte) 0x32,          // BITTYPE
          (byte) 0x68,          // BITNTYPE
          "fixed", 1,
          false,
          false,
          null
  ),
  FLOAT(
          Float.class,
          "real",
          (byte) 0x3B,          // FLT4TYPE
          (byte) 0x6D,          // FLTNTYPE
          "fixed", 4,
          false,
          false,
          null
  ),
  DOUBLE(
          Double.class,
          "float",
          (byte) 0x3E,          // FLT8TYPE
          (byte) 0x6D,
          "fixed", 8,
          false,
          false,
          null
  ),
  BIGDECIMAL(
          BigDecimal.class,
          "decimal(p,s) / numeric(p,s)",
          (byte) 0x6A,          // DECIMALNTYPE / NUMERICNTYPE (same code)
          (byte) 0x6A,          // same for nullable
          "varlen", 0,          // length after precision/scale
          false,
          false,
// Extra bytes: 1 byte precision + 1 byte scale + length byte + BCD value
          "precision:18, scale:10"  // default suggestion; make dynamic if possible
  ),
  STRING(
          String.class,
          "nvarchar(4000) or nvarchar(max)",
          (byte) 0xE7,          // NVARCHARTYPE
          (byte) 0xE7,
          "varlen", 8000,       // max chars (2 bytes per char → 8000*2=16000 bytes)
          true,                 // PLP for (max)
          true,                 // needs TDSCOLLATION (5 bytes)
          "collation:Latin1_General_100_CI_AS_KS_WS_SC_UTF8" // or your default
  ),
  BYTES(
          byte[].class,
          "varbinary(8000) or varbinary(max)",
          (byte) 0xA5,          // BIGVARBINARY
          (byte) 0xA5,
          "varlen", 8000,
          true,                 // PLP for (max)
          false,                // no collation
          null
  ),
  DATE(
          java.sql.Date.class,  // or LocalDate via conversion
          "date",
          (byte) 0x28,          // DATENTYPE
          (byte) 0x28,
          "fixed", 3,
          false,
          false,
          null
  ),
  TIME(
          java.sql.Time.class,  // or LocalTime
          "time(n)",
          (byte) 0x29,          // TIMENTYPE
          (byte) 0x29,
          "varlen", 0,
          false,
          false,
          "scale:7"             // 1 byte scale after XTYPE
  ),
  TIMESTAMP(
          java.sql.Timestamp.class, // or LocalDateTime
          "datetime2(n)",
          (byte) 0x2A,          // DATETIM2TYPE
          (byte) 0x2A,
          "varlen", 0,
          false,
          false,
          "scale:7"             // 1 byte scale after XTYPE
  ),
  CLOB(
          Clob.class,
          "varchar(max)",
          (byte) 0xE7,          // treat as nvarchar(max) for safety, or 0xA7 VARCHAR
          (byte) 0xE7,
          "varlen", -1,         // PLP
          true,
          true,
          "collation: same as STRING"
  ),
  NCLOB(
          NClob.class,
          "nvarchar(max)",
          (byte) 0xE7,
          (byte) 0xE7,
          "varlen", -1,
          true,
          true,
          "collation: required"
  ),
  BLOB(
          Blob.class,
          "varbinary(max)",
          (byte) 0xA5,
          (byte) 0xA5,
          "varlen", -1,
          true,
          false,
          null
  ),
  SQLXML(
          SQLXML.class,
          "xml",
          (byte) 0xF1,          // XMLTYPE
          (byte) 0xF1,
          "varlen", -1,         // PLP-like
          true,
          false,                // schema flag instead of collation
          "schemaPresent: false" // usually 0x01 if schema info sent
  );
  // Fields
  private final Class<?> javaType;
  private final String sqlServerType;
  private final byte xtype;
  private final byte nullableXtype;
  private final String lengthType;      // "fixed" or "varlen"
  private final int defaultMaxLength;   // bytes or chars; -1 = PLP/max
  private final boolean plpSupported;
  private final boolean needsCollation;
  private final String extraConfig;     // e.g. "scale:7" or defaults

  BindingType(Class<?> javaType, String sqlServerType, byte xtype, byte nullableXtype,
              String lengthType, int defaultMaxLength, boolean plpSupported,
              boolean needsCollation, String extraConfig) {
    this.javaType = javaType;
    this.sqlServerType = sqlServerType;
    this.xtype = xtype;
    this.nullableXtype = nullableXtype;
    this.lengthType = lengthType;
    this.defaultMaxLength = defaultMaxLength;
    this.plpSupported = plpSupported;
    this.needsCollation = needsCollation;
    this.extraConfig = extraConfig;
  }

  // Getters...
  public byte getXtype(boolean nullable) {
    return nullable ? nullableXtype : xtype;
  }

  public boolean isPlp() {
    return plpSupported && defaultMaxLength < 0;
  }

  public Class<?> getJavaType() {
    return javaType;
  }

  public String getSqlServerType() {
    return sqlServerType;
  }

  public byte getXtype() {
    return xtype;
  }

  public byte getNullableXtype() {
    return nullableXtype;
  }

  public String getLengthType() {
    return lengthType;
  }

  public int getDefaultMaxLength() {
    return defaultMaxLength;
  }

  public boolean isPlpSupported() {
    return plpSupported;
  }

  public boolean isNeedsCollation() {
    return needsCollation;
  }

  public String getExtraConfig() {
    return extraConfig;
  }
  // etc.
}
//  JDBC Method	Generated SQL Type in @params	TDS Token used for Value
//PreparedRpcQuery bindShort(Short value);
//PreparedRpcQuery bindInteger(Integer value);
//PreparedRpcQuery bindLong(Long value);
//PreparedRpcQuery bindString(String value);
//PreparedRpcQuery bindBytes(byte[] value);
//PreparedRpcQuery bindBoolean(Boolean value);
//PreparedRpcQuery bindSQLXML(SQLXML xml);
//PreparedRpcQuery bindNClob(NClob nclob);
//PreparedRpcQuery bindClob(Clob clob);
//PreparedRpcQuery bindBlob(Blob blob);
//PreparedRpcQuery bindTimestamp(Timestamp ts);
//PreparedRpcQuery bindTime(Time time);
//PreparedRpcQuery bindDate(Date date);
//PreparedRpcQuery bindBigDecimal(BigDecimal bd);
//PreparedRpcQuery bindDouble(Double d);
//PreparedRpcQuery bindFloat(Float f);
//PreparedRpcQuery bindByte(Byte b);
