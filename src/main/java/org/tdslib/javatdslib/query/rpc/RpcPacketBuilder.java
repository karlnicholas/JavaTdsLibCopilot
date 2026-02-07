package org.tdslib.javatdslib.query.rpc;

import io.r2dbc.spi.R2dbcType;
import org.tdslib.javatdslib.TdsType;

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
import java.util.stream.Collectors;

public class RpcPacketBuilder {

  private static final short RPC_PROCID_SPEXECUTESQL = 10;
  private static final byte RPC_PARAM_DEFAULT = 0x00;
  private static final Charset WINDOWS_1252 = Charset.forName("windows-1252");

  // Days between 0001-01-01 and 1970-01-01
  private static final long DAYS_TO_1970 = 719162;

  private final String sql;
  private final List<ParamEntry> params;
  private final boolean update;

  public RpcPacketBuilder(String sql, List<ParamEntry> params, boolean update) {
    this.sql = sql;
    this.params = params;
    this.update = update;
  }

  public ByteBuffer buildRpcPacket() {
    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
    buf.order(ByteOrder.LITTLE_ENDIAN);

    writeRpcHeader(buf);

    // 1. @stmt
    writeParamName(buf, "@stmt");
    buf.put(RPC_PARAM_DEFAULT);
    buf.put((byte) TdsType.NVARCHAR.byteVal);
    buf.putShort((short) -1);
    writeCollation(buf);

    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
    buf.putShort((short) sqlBytes.length);
    buf.put(sqlBytes);

    // 2. @params
    if (!params.isEmpty()) {
      writeParamName(buf, "@params");
      buf.put(RPC_PARAM_DEFAULT);
      buf.put((byte) TdsType.NVARCHAR.byteVal);
      buf.putShort((short) -1);
      writeCollation(buf);

      String paramDecl = buildParamDecl();
      byte[] declBytes = paramDecl.getBytes(StandardCharsets.UTF_16LE);
      buf.putShort((short) declBytes.length);
      buf.put(declBytes);

      // 3. Values
      for (ParamEntry param : params) {
        writeParam(buf, param);
      }
    }

    buf.flip();
    return buf;
  }

  private void writeRpcHeader(ByteBuffer buf) {
    buf.putShort((short) 0xFFFF);
    buf.putShort(RPC_PROCID_SPEXECUTESQL);
    buf.putShort((short) 0);
  }

  private void writeParam(ByteBuffer buf, ParamEntry param) {
    writeParamName(buf, param.key().name());
    buf.put(RPC_PARAM_DEFAULT);

    TdsType type = param.key().type();
    buf.put((byte) type.byteVal);

    switch (type.strategy) {
      case FIXED:
        break;

      case BYTELEN:
        buf.put((byte) type.fixedSize);
        if (type == TdsType.DECIMALN || type == TdsType.NUMERICN) {
          buf.put((byte) 18);
          buf.put((byte) 0);
        }
        break;

      case USHORTLEN:
        buf.putShort((short) 8000);
        if (type == TdsType.NVARCHAR || type == TdsType.BIGVARCHR) {
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

    writeValue(buf, type, param.value());
  }

  private void writeValue(ByteBuffer buf, TdsType type, Object value) {
    if (value == null) {
      writeNull(buf, type);
      return;
    }

    // FIX 1: Handle GUID explicitly (prevents ClassCastException)
    if (type == TdsType.GUID) {
      buf.put((byte) 16); // Length
      UUID u = (UUID) value;
      // Write as two Little-Endian longs to match TdsRowImpl reading logic
      buf.putLong(u.getMostSignificantBits());
      buf.putLong(u.getLeastSignificantBits());
      return;
    }

    switch (type.r2dbcType) {
      case TINYINT:
        buf.put((byte) 1);
        buf.put(((Number) value).byteValue());
        break;
      case SMALLINT:
        buf.put((byte) 2);
        buf.putShort(((Number) value).shortValue());
        break;
      case INTEGER:
        buf.put((byte) 4);
        buf.putInt(((Number) value).intValue());
        break;
      case BIGINT:
        buf.put((byte) 8);
        buf.putLong(((Number) value).longValue());
        break;

      case REAL:
        buf.put((byte) 4);
        buf.putFloat(((Number) value).floatValue());
        break;
      case DOUBLE:
        buf.put((byte) 8);
        buf.putDouble(((Number) value).doubleValue());
        break;

      case BOOLEAN:
        buf.put((byte) 1);
        boolean b = (value instanceof Boolean) ? (Boolean) value : ((Number) value).intValue() != 0;
        buf.put((byte) (b ? 1 : 0));
        break;

      case DECIMAL:
      case NUMERIC:
        byte[] decBytes = convertToDecimalBytes(value, (byte)18, (byte)0);
        buf.put((byte) decBytes.length);
        buf.put(decBytes);
        break;

      case VARCHAR:
      case CHAR:
        byte[] ascii = ((String) value).getBytes(WINDOWS_1252);
        buf.putShort((short) ascii.length);
        buf.put(ascii);
        break;

      case NVARCHAR:
      case NCHAR:
        byte[] utf16 = ((String) value).getBytes(StandardCharsets.UTF_16LE);
        buf.putShort((short) utf16.length);
        buf.put(utf16);
        break;

      case BINARY:
      case VARBINARY:
        byte[] bin = convertToBytes(value);
        buf.putShort((short) bin.length);
        buf.put(bin);
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
        long odtTicks = odt.toLocalTime().toNanoOfDay() / 100;
        long odtDays = odt.toLocalDate().toEpochDay() + DAYS_TO_1970;
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

  private String buildParamDecl() {
    return params.stream()
        .map(p -> p.key().name() + " " + getSqlTypeDeclaration(p.key().type()))
        .collect(Collectors.joining(", "));
  }

  private String getSqlTypeDeclaration(TdsType type) {
    // FIX 2: Return correct SQL type for GUID
    if (type == TdsType.GUID) return "uniqueidentifier";

    if (type == TdsType.NVARCHAR) return "nvarchar(max)";
    if (type == TdsType.INTN) return "int";
    if (type == TdsType.BITN) return "bit";
    if (type == TdsType.DATE) return "date";
    if (type == TdsType.TIME) return "time(7)";
    if (type == TdsType.DATETIME2) return "datetime2(7)";
    if (type == TdsType.DATETIMEOFFSET) return "datetimeoffset(7)";

    return type.r2dbcType.name().toLowerCase();
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
}