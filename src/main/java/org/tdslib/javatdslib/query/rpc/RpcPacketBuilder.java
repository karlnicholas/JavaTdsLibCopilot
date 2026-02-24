package org.tdslib.javatdslib.query.rpc;

import io.r2dbc.spi.Parameter;
import org.tdslib.javatdslib.TdsType;
import org.tdslib.javatdslib.headers.AllHeaders;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class RpcPacketBuilder {

  private static final short RPC_PROCID_SPEXECUTESQL = 10;
  private static final byte RPC_PARAM_DEFAULT = 0x00;
  private static final Charset WINDOWS_1252 = Charset.forName("windows-1252");
  private static final long DAYS_TO_1970 = 719162;

  private final String sql;
  private final List<List<ParamEntry>> batchParams; // NOW TAKES A LIST OF BATCHES
  private final boolean update;

  public RpcPacketBuilder(String sql, List<List<ParamEntry>> batchParams, boolean update) {
    this.sql = sql;
    this.batchParams = batchParams;
    this.update = update;
  }

  public ByteBuffer buildRpcPacket() {
    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024); // Large buffer for pipelining
    buf.order(ByteOrder.LITTLE_ENDIAN);

    for (int i = 0; i < batchParams.size(); i++) {
      List<ParamEntry> params = batchParams.get(i);

      // TDS Spec: 0x80 (BatchFlag) separates multiple RPCReqBatch requests
      if (i > 0) {
        buf.put((byte) 0xFF);
      }

      writeRpcHeader(buf);

      // 1. @stmt
      writeParamName(buf, "@stmt");
      buf.put(RPC_PARAM_DEFAULT);
      buf.put((byte) TdsType.NVARCHAR.byteVal);
      buf.putShort((short) 8000);
      writeCollation(buf);

      byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
      buf.putShort((short) sqlBytes.length);
      buf.put(sqlBytes);

      // 2. @params
      if (!params.isEmpty()) {
        writeParamName(buf, "@params");
        buf.put(RPC_PARAM_DEFAULT);
        buf.put((byte) TdsType.NVARCHAR.byteVal);
        buf.putShort((short) 8000);
        writeCollation(buf);

        String paramDecl = buildParamDecl(params);
        byte[] declBytes = paramDecl.getBytes(StandardCharsets.UTF_16LE);
        buf.putShort((short) declBytes.length);
        buf.put(declBytes);

        // 3. Values
        for (ParamEntry param : params) {
          writeParam(buf, param);
        }
      }
    }

    buf.flip();
    if (update) {
      byte[] allHeadersBytes = AllHeaders.forAutoCommit(1).toBytes();
      ByteBuffer fullPayload = ByteBuffer.allocate(allHeadersBytes.length + buf.limit())
          .order(ByteOrder.LITTLE_ENDIAN);
      fullPayload.put(allHeadersBytes);
      fullPayload.put(buf);
      fullPayload.flip();
      return fullPayload;
    }
    return buf;
  }

  private void writeRpcHeader(ByteBuffer buf) {
    buf.putShort((short) 0xFFFF);
    buf.putShort(RPC_PROCID_SPEXECUTESQL);
    buf.putShort((short) 0);
  }

  // --- Parameter Declaration ---

  private String buildParamDecl(List<ParamEntry> params) {
    if (params.isEmpty()) return "";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < params.size(); i++) {
      ParamEntry entry = params.get(i);
      if (i > 0) sb.append(",");

      String decl = getSqlTypeDeclaration(entry);
      sb.append(entry.key().name()).append(" ").append(decl);
      if (entry.value() instanceof Parameter.Out) {
        sb.append(" output");
      }
    }
    return sb.toString();
  }

  // ... (The rest of the class remains exactly the same: getSqlTypeDeclaration, writeParam, writeValue, etc.)

  private String getSqlTypeDeclaration(ParamEntry entry) {
    TdsType type = entry.key().type();
    Object value = entry.value().getValue();

    if (type == TdsType.INT1) return "tinyint";
    if (type == TdsType.INT2) return "smallint";
    if (type == TdsType.INT4) return "int";
    if (type == TdsType.INT8) return "bigint";

    if (type == TdsType.INTN) {
      if (value instanceof Long) return "bigint";
      if (value instanceof Short) return "smallint";
      if (value instanceof Byte) return "tinyint";
      return "int";
    }

    if (type == TdsType.DECIMAL || type == TdsType.NUMERIC ||
        type == TdsType.DECIMALN || type == TdsType.NUMERICN) {
      int p = 38;
      int s = 0;
      if (value instanceof BigDecimal bd) s = getDecimalScale(bd);
      return String.format("decimal(%d,%d)", p, s);
    }

    if (type == TdsType.FLT4 || type == TdsType.REAL) return "real";
    if (type == TdsType.FLT8 || type == TdsType.FLTN) return "float";

    if (type == TdsType.NVARCHAR || type == TdsType.NCHAR || type == TdsType.NTEXT) {
      return isLargeString(entry) ? "nvarchar(max)" : "nvarchar(4000)";
    }
    if (type == TdsType.BIGVARCHR || type == TdsType.BIGCHAR ||
        type == TdsType.VARCHAR || type == TdsType.CHAR || type == TdsType.TEXT) {
      return isLargeString(entry) ? "varchar(max)" : "varchar(8000)";
    }
    if (type == TdsType.BIGVARBIN || type == TdsType.BIGBINARY ||
        type == TdsType.VARBINARY || type == TdsType.IMAGE || type == TdsType.BINARY) {
      return isLargeBinary(entry) ? "varbinary(max)" : "varbinary(8000)";
    }

    if (type == TdsType.DATETIME2 || type == TdsType.DATETIME) return "datetime2";
    if (type == TdsType.DATE) return "date";
    if (type == TdsType.TIME) return "time";
    if (type == TdsType.DATETIMEOFFSET) return "datetimeoffset";

    if (type == TdsType.GUID) return "uniqueidentifier";
    if (type == TdsType.BITN || type == TdsType.BIT) return "bit";

    return type.r2dbcType.name().toLowerCase();
  }

  private boolean isLargeString(ParamEntry entry) {
    Object val = entry.value().getValue();
    if (val instanceof String s) return s.length() > 4000;
    return false;
  }

  private boolean isLargeBinary(ParamEntry entry) {
    Object val = entry.value().getValue();
    int len = 0;
    if (val instanceof ByteBuffer bb) len = bb.remaining();
    else if (val instanceof byte[] b) len = b.length;
    return len > 8000;
  }

  private void writeParam(ByteBuffer buf, ParamEntry param) {
    writeParamName(buf, param.key().name());
    if ( param.value() instanceof Parameter.Out) {
      buf.put((byte) 0x01);
    } else {
      buf.put(RPC_PARAM_DEFAULT);
    }

    TdsType type = param.key().type();

    if (type == TdsType.INT1 || type == TdsType.INT2 || type == TdsType.INT4 || type == TdsType.INT8) {
      buf.put((byte) TdsType.INTN.byteVal);
      byte maxLen = 4;
      Object val = param.value().getValue();
      if (type == TdsType.INT1 || val instanceof Byte) maxLen = 1;
      else if (type == TdsType.INT2 || val instanceof Short) maxLen = 2;
      else if (type == TdsType.INT8 || val instanceof Long) maxLen = 8;
      buf.put(maxLen);
      writeValue(buf, TdsType.INTN, val);
      return;
    }

    if (type == TdsType.FLT4 || type == TdsType.REAL) {
      buf.put((byte) TdsType.FLTN.byteVal);
      buf.put((byte) 4);
      writeValue(buf, type, param.value().getValue());
      return;
    }
    if (type == TdsType.FLT8) {
      buf.put((byte) TdsType.FLTN.byteVal);
      buf.put((byte) 8);
      writeValue(buf, type, param.value().getValue());
      return;
    }

    buf.put((byte) type.byteVal);

    switch (type.strategy) {
      case FIXED:
        break;

      case BYTELEN:
        byte len = (byte) type.fixedSize;
        if (type == TdsType.INTN) {
          Object val = param.value().getValue();
          if (val instanceof Long) len = 8;
          else if (val instanceof Integer) len = 4;
          else if (val instanceof Short) len = 2;
          else if (val instanceof Byte) len = 1;
          else len = 4;
        } else if (type == TdsType.FLTN) {
          if (param.value().getValue() instanceof Float) len = 4;
          else len = 8;
        }
        buf.put(len);
        if (type == TdsType.DECIMALN || type == TdsType.NUMERICN) {
          byte p = 38;
          byte s = 0;
          if (param.value().getValue() instanceof BigDecimal bd) s = (byte) getDecimalScale(bd);
          buf.put(p);
          buf.put(s);
        }
        break;

      case USHORTLEN:
        int encodedLen = getEncodedLength(type, param.value().getValue());

        if (encodedLen > 8000) {
          buf.putShort((short) -1);
        } else {
          buf.putShort((short) 8000);
        }

        if (type == TdsType.NVARCHAR || type == TdsType.BIGVARCHR ||
            type == TdsType.NCHAR || type == TdsType.CHAR || type == TdsType.BIGCHAR ||
            type == TdsType.VARCHAR || type == TdsType.TEXT) {
          writeCollation(buf);
        }
        break;

      case SCALE_LEN:
        buf.put((byte) 7);
        break;

      case PLP:
        buf.put((byte) 0);
        break;

      case LONGLEN:
        break;
    }

    writeValue(buf, type, param.value().getValue());
  }

  private void writeValue(ByteBuffer buf, TdsType type, Object value) {
    if (value == null) {
      writeNull(buf, type);
      return;
    }

    if (type == TdsType.INTN) {
      if (value instanceof Long) { buf.put((byte) 8); buf.putLong((Long) value); return; }
      if (value instanceof Integer) { buf.put((byte) 4); buf.putInt((Integer) value); return; }
      if (value instanceof Short) { buf.put((byte) 2); buf.putShort((Short) value); return; }
      if (value instanceof Byte) { buf.put((byte) 1); buf.put((Byte) value); return; }
    }
    if (type == TdsType.GUID) {
      buf.put((byte) 16);
      UUID u = (UUID) value;
      buf.putLong(u.getMostSignificantBits());
      buf.putLong(u.getLeastSignificantBits());
      return;
    }

    switch (type.r2dbcType) {
      case TINYINT: buf.put((byte) 1); buf.put(((Number) value).byteValue()); break;
      case SMALLINT: buf.put((byte) 2); buf.putShort(((Number) value).shortValue()); break;
      case INTEGER: buf.put((byte) 4); buf.putInt(((Number) value).intValue()); break;
      case BIGINT: buf.put((byte) 8); buf.putLong(((Number) value).longValue()); break;
      case REAL: buf.put((byte) 4); buf.putFloat(((Number) value).floatValue()); break;
      case DOUBLE: buf.put((byte) 8); buf.putDouble(((Number) value).doubleValue()); break;
      case BOOLEAN:
        buf.put((byte) 1);
        boolean b = (value instanceof Boolean) ? (Boolean) value : ((Number) value).intValue() != 0;
        buf.put((byte) (b ? 1 : 0));
        break;
      case DECIMAL:
      case NUMERIC:
        byte prec = 38;
        byte scale = 0;
        if (value instanceof BigDecimal bd) scale = (byte) getDecimalScale(bd);
        byte[] decBytes = convertToDecimalBytes(value, prec, scale);
        buf.put((byte) decBytes.length);
        buf.put(decBytes);
        break;

      case VARCHAR:
      case CHAR:
        String sVal = (value instanceof String) ? (String) value : value.toString();
        byte[] ascii = sVal.getBytes(WINDOWS_1252);

        if (ascii.length > 8000) {
          writePlp(buf, ascii);
        } else {
          buf.putShort((short) ascii.length);
          buf.put(ascii);
        }
        break;

      case NVARCHAR:
      case NCHAR:
        String nsVal = (value instanceof String) ? (String) value : value.toString();
        byte[] utf16 = nsVal.getBytes(StandardCharsets.UTF_16LE);

        if (utf16.length > 8000) {
          writePlp(buf, utf16);
        } else {
          buf.putShort((short) utf16.length);
          buf.put(utf16);
        }
        break;

      case BINARY:
      case VARBINARY:
        byte[] bin = convertToBytes(value);
        if (bin.length > 8000) {
          writePlp(buf, bin);
        } else {
          buf.putShort((short) bin.length);
          buf.put(bin);
        }
        break;

      case DATE:
        LocalDate d = (LocalDate) value;
        long days = d.toEpochDay() + DAYS_TO_1970;
        buf.put((byte) 3);
        write3ByteInt(buf, (int) days);
        break;
      case TIME:
        LocalTime t = (LocalTime) value;
        long tTicks = t.toNanoOfDay() / 100;
        buf.put((byte) 5);
        write5ByteInt(buf, tTicks);
        break;
      case TIMESTAMP:
        LocalDateTime ldt = (LocalDateTime) value;
        long dtTicks = ldt.toLocalTime().toNanoOfDay() / 100;
        long dtDays = ldt.toLocalDate().toEpochDay() + DAYS_TO_1970;
        buf.put((byte) 8);
        write5ByteInt(buf, dtTicks);
        write3ByteInt(buf, (int) dtDays);
        break;
      case TIMESTAMP_WITH_TIME_ZONE:
        OffsetDateTime odt = (OffsetDateTime) value;
        OffsetDateTime utcOdt = odt.withOffsetSameInstant(java.time.ZoneOffset.UTC);

        long odtTicks = utcOdt.toLocalTime().toNanoOfDay() / 100;
        long odtDays = utcOdt.toLocalDate().toEpochDay() + DAYS_TO_1970;

        int offsetMins = odt.getOffset().getTotalSeconds() / 60;

        buf.put((byte) 10);
        write5ByteInt(buf, odtTicks);
        write3ByteInt(buf, (int) odtDays);
        buf.putShort((short) offsetMins);
        break;

      default:
        throw new IllegalArgumentException("Unsupported Type for Serialization: " + type.r2dbcType);
    }
  }

  private void writeNull(ByteBuffer buf, TdsType type) {
    switch (type.strategy) {
      case BYTELEN -> buf.put((byte) 0);
      case USHORTLEN -> buf.putShort((short) 0xFFFF);
      case PLP -> buf.putLong(0xFFFFFFFFFFFFFFFFL);
      default -> buf.put((byte) 0);
    }
  }

  private void writeParamName(ByteBuffer buf, String name) {
    if (name == null || name.isEmpty()) {
      buf.put((byte) 0);
      return;
    }
    buf.put((byte) name.length());
    buf.put(name.getBytes(StandardCharsets.UTF_16LE));
  }

  private void writeCollation(ByteBuffer buf) {
    buf.put(new byte[]{0x09, 0x04, (byte) 0xD0, 0x00, 0x34});
  }

  private byte[] convertToBytes(Object value) {
    if (value instanceof byte[]) return (byte[]) value;
    if (value instanceof ByteBuffer bb) {
      byte[] b = new byte[bb.remaining()];
      bb.duplicate().get(b);
      return b;
    }
    return new byte[0];
  }

  private byte[] convertToDecimalBytes(Object value, Byte prec, Byte scale) {
    BigDecimal bd = ((BigDecimal) value).setScale(scale, RoundingMode.HALF_UP);
    BigInteger unscaled = bd.unscaledValue();
    byte[] bytes = unscaled.toByteArray();
    byte sign = (byte) (unscaled.signum() >= 0 ? 1 : 0);
    byte[] res = new byte[bytes.length + 1];
    res[0] = sign;
    for (int i = 0; i < bytes.length; i++) {
      res[i + 1] = bytes[bytes.length - 1 - i];
    }
    return res;
  }

  private void write3ByteInt(ByteBuffer buf, int val) {
    buf.put((byte) (val & 0xFF));
    buf.put((byte) ((val >> 8) & 0xFF));
    buf.put((byte) ((val >> 16) & 0xFF));
  }

  private void write5ByteInt(ByteBuffer buf, long val) {
    buf.put((byte) (val & 0xFF));
    buf.put((byte) ((val >> 8) & 0xFF));
    buf.put((byte) ((val >> 16) & 0xFF));
    buf.put((byte) ((val >> 24) & 0xFF));
    buf.put((byte) ((val >> 32) & 0xFF));
  }

  private int getDecimalScale(BigDecimal bd) {
    return Math.max(0, Math.min(38, bd.scale()));
  }

  private int getEncodedLength(TdsType type, Object value) {
    if (value == null) return 0;
    if (value instanceof byte[]) return ((byte[]) value).length;
    if (value instanceof ByteBuffer) return ((ByteBuffer) value).remaining();
    if (value instanceof String s) {
      if (type == TdsType.NVARCHAR || type == TdsType.NCHAR || type == TdsType.NTEXT) {
        return s.length() * 2;
      }
      return s.getBytes(WINDOWS_1252).length;
    }
    return 0;
  }

  private void writePlp(ByteBuffer buf, byte[] data) {
    buf.putLong(data.length);
    buf.putInt(data.length);
    buf.put(data);
    buf.putInt(0);
  }
}