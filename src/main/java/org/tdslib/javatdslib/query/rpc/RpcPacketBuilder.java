package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.headers.AllHeaders;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RpcPacketBuilder {

  private static final short RPC_PROCID_SWITCH = (short) 0xFFFF;
  private static final short RPC_PROCID_SPEXECUTESQL = 10;

  private static final byte RPC_PARAM_DEFAULT = 0x00;

  private final String sql;
  private final List<ParamEntry> params;
  private final boolean update;

  public RpcPacketBuilder(String sql, List<ParamEntry> params, boolean update) {
    this.sql = sql;
    this.params = params;
    this.update = update;
  }

  public ByteBuffer buildRpcPacket() {
    ByteBuffer buf = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);

    // RPC header
    buf.putShort(RPC_PROCID_SWITCH);
    buf.putShort(RPC_PROCID_SPEXECUTESQL);
    buf.putShort((short) 0x0000);

    // Param 1: @stmt
    putParamName(buf, "@stmt");
    buf.put(RPC_PARAM_DEFAULT);
    putTypeInfoNVarcharMax(buf);
    putPlpUnicodeString(buf, sql);

    // Param 2: @params — entry for every parameter
    String paramsDecl = buildParamsDeclaration();
    putParamName(buf, "@params");
    buf.put(RPC_PARAM_DEFAULT);
    putTypeInfoNVarcharMax(buf);
    putPlpUnicodeString(buf, paramsDecl);

    // All parameters — use real name or empty
    for (int i = 0; i < params.size(); i++) {
      ParamEntry entry = params.get(i);
      String rpcParamName = entry.key().name();

      putParamName(buf, rpcParamName);                        // empty for unnamed
      buf.put(RPC_PARAM_DEFAULT);
      putTypeInfoForParam(buf, entry);
      putParamValue(buf, entry);
    }

    buf.flip();
    if ( update ) {
      // Now build ALL_HEADERS (most common: auto-commit, transaction=0, outstanding=1)
      byte[] allHeadersBytes = AllHeaders.forAutoCommit(1).toBytes();
      // Combine: ALL_HEADERS + RPC core
      ByteBuffer fullPayload = ByteBuffer.allocate(allHeadersBytes.length + buf.limit())
          .order(ByteOrder.LITTLE_ENDIAN);
      fullPayload.put(allHeadersBytes);
      fullPayload.put(buf);

      fullPayload.flip();
      return fullPayload;
    } else {
      return buf;
    }
  }

  private String buildParamsDeclaration() {
    if (params.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < params.size(); i++) {
      ParamEntry entry = params.get(i);
      String declName = entry.key().name();
      String typeDecl = entry.key().type().getSqlTypeName();

      if (i > 0) sb.append(", ");
      sb.append(declName).append(" ").append(typeDecl);
    }
    return sb.toString();
  }

//  // Name used in @params declaration — always non-empty
//  private String getDeclarationName(ParamEntry entry, int index) {
//    String name = entry.key().name();
//    if (name != null && !name.isEmpty()) {
//      return name;
//    }
//    return "";  // dummy for unnamed in declaration
//  }

  // Name used in actual RPC parameter token — empty for unnamed
//  private String getRpcParamName(ParamEntry entry) {
//    String name = entry.key().name();
//    if (name != null && !name.isEmpty()) {
//      return name.startsWith("@") ? name : "@" + name;
//    }
//    return "";  // truly unnamed in TDS packet
//  }

//  private String getSqlTypeDeclaration(BindingType bt) {
//    return switch (bt) {
//      case SHORT      -> "smallint";
//      case INTEGER    -> "int";
//      case LONG       -> "bigint";
//      case BYTE       -> "tinyint";
//      case BOOLEAN    -> "bit";
//      case FLOAT      -> "real";
//      case DOUBLE     -> "float";
//      case BIGDECIMAL -> "decimal(38,10)";
//      case STRING     -> "nvarchar(500)";
//      case BYTES      -> "varbinary(8000)";
//      case DATE       -> "date";
//      case TIME       -> "time(7)";
//      case TIMESTAMP  -> "datetime2(7)";
//      case CLOB       -> "varchar(max)";
//      case NCLOB      -> "nvarchar(max)";
//      case BLOB       -> "varbinary(max)";
//      case SQLXML     -> "xml";
//    };
//  }

  // Helpers (unchanged)
  private void putParamName(ByteBuffer buf, String name) {
    byte[] bytes = name.getBytes(StandardCharsets.UTF_16LE);
//    buf.putShort((short) (bytes.length / 2));
    buf.put((byte) (bytes.length / 2));
    buf.put(bytes);
  }

  private void putTypeInfoNVarcharMax(ByteBuffer buf) {
    buf.put((byte) 0xE7);
    buf.putShort((short) 4000);
    buf.putInt(0x00000409);
//    buf.put((byte) 0x00);
    buf.put((byte) 52);
  }

  private void putPlpUnicodeString(ByteBuffer buf, String str) {
    byte[] bytes = str.getBytes(StandardCharsets.UTF_16LE);
//    buf.putInt(-1);
    buf.putShort((short) bytes.length);
    buf.put(bytes);
//    buf.putInt(0);
  }

  private void putTypeInfoIntNType(ByteBuffer buf, ParamEntry entry) {
    buf.put((byte) 8);
    buf.putLong((Long)entry.value());
  }

  private void putTypeInfoForParam(ByteBuffer buf, ParamEntry entry) {
    BindingType type = entry.key().type();
    byte xtype = type.getTdsXType();
    buf.put(xtype);

    switch (type.getTypeStyle()) {
      case FIXED -> {
        // Nothing extra
      }
      case LENGTH -> {
        Number len = type.getLength();
        if (len == null) {
          throw new IllegalStateException("LENGTH style requires a length value for type: " + type.getSqlTypeName());
        }
        buf.put(len.byteValue());
      }
      case PREC_SCALE -> {
        Byte prec = type.getPrecision();
        Byte scale = type.getScale();
        if (prec == null || scale == null) {
          throw new IllegalStateException("PREC_SCALE style requires precision and scale values for type: " + type.getSqlTypeName());
        }
        buf.put(prec);
        buf.put(scale);
      }
      case VARLEN -> {
        Number maxLen = type.getLength();
        if (maxLen == null) {
          throw new IllegalStateException("VARLEN style requires a max length value for type: " + type.getSqlTypeName());
        }
        buf.putShort(maxLen.shortValue());

        // Additional handling for specific VARLEN types
        if (type == BindingType.CHAR || type == BindingType.VARCHAR || type == BindingType.TEXT ||
            type == BindingType.NCHAR || type == BindingType.NVARCHAR || type == BindingType.NTEXT) {
          // Collation info (hardcoded default: LCID 0x00000409 = English US, flags 0x00, sort ID 52 = Latin1_General_CI_AS)
          buf.putInt(0x00000409);  // LCID (4 bytes)
          buf.put((byte) 0x00);    // Flags (1 byte)
        } else if (type == BindingType.XML) {
          // Schema collection name length (0 for no schema)
          buf.put((byte) 0x00);
        }
        // For BINARY/VARBINARY/IMAGE: no extra after max length
      }
      default -> {
        throw new IllegalStateException("Unsupported TypeStyle: " + type.getTypeStyle());
      }
    }
  }

  private void putParamValue(ByteBuffer buf, ParamEntry entry) {
    BindingType type = entry.key().type();
    Object value = entry.value();

    // Handle NULL value uniformly for all types
    if (value == null) {
      buf.put((byte) 0xFF);  // NULL indicator for most types
      return;
    }

    switch (type.getTypeStyle()) {
      case FIXED -> {
        // Write the value directly (no length prefix, but fixed size implied)
        writeFixedValue(buf, type, value);
      }
      case LENGTH -> {
        Number fixedLen = type.getLength();
        if (fixedLen == null) {
          throw new IllegalStateException("LENGTH style requires a fixed length for type: " + type.getSqlTypeName());
        }
        buf.put(fixedLen.byteValue());  // 1-byte length
        writeFixedLengthValue(buf, type, value, fixedLen.byteValue());
      }
      case PREC_SCALE -> {
        // Length is variable based on value digits, but send actual data length first
        byte[] decimalBytes = convertToDecimalBytes(value, type.getPrecision(), type.getScale());
        buf.put((byte) decimalBytes.length);  // 1-byte actual length
        buf.put(decimalBytes);
      }
//      case VARLEN -> {
//        byte[] dataBytes = convertToVarlenBytes(type, value);
//        Number maxLen = type.getLength();
//        if (maxLen == null) {
//          throw new IllegalStateException("VARLEN style requires a max length for type: " + type.getSqlTypeName());
//        }
//        short max = maxLen.shortValue();
//        if (max == -1) {  // PLP (MAX)
//          writePlpHeader(buf, dataBytes);
//          buf.put(dataBytes);
//        } else {
//          buf.putShort((short) dataBytes.length);  // 2-byte actual length
//          buf.put(dataBytes);
//        }
//      }
      case VARLEN -> {
        byte[] dataBytes = convertToVarlenBytes(type, value);

        Number maxLen = type.getLength();
        if (maxLen == null) {
          throw new IllegalStateException("VARLEN requires max length");
        }
        short max = maxLen.shortValue();

        if (max == -1) {  // MAX / PLP
          // PLP header
//          buf.put((byte) 0xFE);

          buf.putLong(0xFFFFFFFFFFFFFFFEL);
          // For simple case: single chunk (most common for small/medium data)
          buf.putInt(dataBytes.length);   // chunk size (4-byte)
          buf.put(dataBytes);             // data
          buf.putInt(0);                  // terminator chunk (0 length)

        } else {
          // Fixed max length (non-PLP)
          buf.putShort((short) dataBytes.length);
          buf.put(dataBytes);
        }
      }
      default -> {
        throw new IllegalStateException("Unsupported TypeStyle for value writing: " + type.getTypeStyle());
      }
    }
  }

// Helper methods (implement these based on your TDS writer utils)

  // For FIXED types (e.g. DATE, TIME, DATETIME)
  private void writeFixedValue(ByteBuffer buf, BindingType type, Object value) {
    // Convert value to fixed bytes based on type
    if (type == BindingType.DATE) {
      // Convert LocalDate to 3-byte TDS date, etc.
      // Example placeholder
      buf.putInt(0);  // dummy
    } else if (type == BindingType.UNIQUEIDENTIFIER) {
      // UUID to 16-byte, etc.
      buf.put((byte[]) value);  // assume byte[16]
    } // Add cases for each FIXED type
  }

  // For LENGTH types (e.g. INT, BIGINT, FLOAT, REAL)
  private void writeFixedLengthValue(ByteBuffer buf, BindingType type, Object value, byte length) {
    // Write the value bytes padded/fitted to exact length
    if (type == BindingType.INT) {
      buf.putInt((Integer) value);
    } else if (type == BindingType.BIGINT) {
      buf.putLong((Long) value);
    } else if (type == BindingType.FLOAT) {
      buf.putDouble((Double) value);
    } else if (type == BindingType.REAL) {
      buf.putFloat((Float) value);
    } else if (type == BindingType.BIT) {
      buf.put((byte) ((Boolean) value ? 1 : 0));
    } // Add more
  }

  // For PREC_SCALE (decimal/numeric) — convert BigDecimal to scaled integer bytes
  private byte[] convertToDecimalBytes(Object value, Byte maxPrec, Byte scale) {
    if (!(value instanceof BigDecimal bd)) {
      throw new IllegalArgumentException("Expected BigDecimal for " + BindingType.DECIMAL.getSqlTypeName());
    }
    BigDecimal scaled = bd.setScale(scale, RoundingMode.HALF_UP);
    BigInteger unscaled = scaled.unscaledValue();
    byte[] bytes = unscaled.toByteArray();
    // Pad or truncate to fit precision, add sign byte (0x00 positive, 0x01 negative)
    byte[] result = new byte[bytes.length + 1];
    result[0] = (byte) (unscaled.signum() >= 0 ? 0x00 : 0x01);
    System.arraycopy(bytes, 0, result, 1, bytes.length);
    return result;
  }

  // For VARLEN types — convert to bytes
  private byte[] convertToVarlenBytes(BindingType type, Object value) {
    if (type == BindingType.VARCHAR || type == BindingType.NVARCHAR || type == BindingType.CHAR ||
        type == BindingType.NCHAR || type == BindingType.TEXT || type == BindingType.NTEXT) {
      return ((String) value).getBytes(StandardCharsets.UTF_16LE);  // or UTF-8 for VARCHAR
    } else if (type == BindingType.BINARY || type == BindingType.VARBINARY || type == BindingType.IMAGE) {
      return (byte[]) value;
    } else if (type == BindingType.XML) {
      return ((String) value).getBytes(StandardCharsets.UTF_16LE);  // XML as nvarchar
    } else if (type == BindingType.GEOGRAPHY || type == BindingType.GEOMETRY) {
      // Custom conversion for spatial types (e.g. from WKB)
      return (byte[]) value;  // assume byte[]
    }
    throw new IllegalStateException("No VARLEN conversion for type: " + type.getSqlTypeName());
  }

//  // For PLP (VARLEN MAX) — write PLP header
//  private void writePlpHeader(ByteBuffer buf, byte[] data) {
//    // PLP header: 0xFE (PLP marker) + 4-byte length + data chunks (simple single chunk here)
////    buf.put((byte) 0xFE);
////    buf.putInt(data.length);
//    buf.putLong(-1);
//    buf.putInt(data.length);
//    // Then the data follows directly
//    // For large data, chunk it, but for simplicity assume small
//  }
}