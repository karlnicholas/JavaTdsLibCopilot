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
    // 1. Calculate approximate size (start with 4KB, resize if needed)
    // For production, you'd calculate exact size to avoid reallocation
    ByteBuffer buf = ByteBuffer.allocate(8192);
    buf.order(ByteOrder.LITTLE_ENDIAN);

    // 2. Write RPC Header for sp_executesql
    writeRpcHeader(buf);

    // 3. Parameter 1: @stmt (The SQL Query)
    writeParamName(buf, "@stmt");
    buf.put(RPC_PARAM_DEFAULT); // Status

    // @stmt Type Info: NVARCHAR(MAX)
    buf.put((byte) TdsType.NVARCHAR.byteVal);
    buf.putShort((short) -1); // 0xFFFF = MAX
    writeCollation(buf);

    // @stmt Value
    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
    buf.putShort((short) sqlBytes.length);
    buf.put(sqlBytes);

    // 4. Parameter 2: @params (The declaration string)
    // e.g. "@p0 int, @p1 bit"
    if (!params.isEmpty()) {
      writeParamName(buf, "@params");
      buf.put(RPC_PARAM_DEFAULT); // Status

      // @params Type Info: NVARCHAR(MAX)
      buf.put((byte) TdsType.NVARCHAR.byteVal);
      buf.putShort((short) -1); // MAX
      writeCollation(buf);

      // @params Value
      String paramDecl = buildParamDecl();
      byte[] declBytes = paramDecl.getBytes(StandardCharsets.UTF_16LE);
      buf.putShort((short) declBytes.length);
      buf.put(declBytes);

      // 5. Write Actual Parameter Values (@p0, @p1...)
      for (ParamEntry param : params) {
        writeParam(buf, param);
      }
    }

    buf.flip();
    return buf;
  }

  private void writeRpcHeader(ByteBuffer buf) {
    // Length of Procedure Name (0xFFFF means we use ProcID)
    buf.putShort((short) 0xFFFF);
    // ProcID for sp_executesql
    buf.putShort(RPC_PROCID_SPEXECUTESQL);
    // Option Flags (0x0000)
    buf.putShort((short) 0);
  }

  private void writeParam(ByteBuffer buf, ParamEntry param) {
    // 1. Name
    writeParamName(buf, param.key().name());

    // 2. Status
    buf.put(RPC_PARAM_DEFAULT);

    // 3. Type Info
    TdsType type = param.key().type();
    buf.put((byte) type.byteVal);

    // 4. Type Meta (Length, Precision, Scale, Collation) based on Strategy
    switch (type.strategy) {
      case FIXED:
        // No extra metadata for pure fixed types
        break;

      case BYTELEN:
        // Writes the "Max Length" byte (e.g. 4 for INTN, 8 for FLTN)
        buf.put((byte) type.fixedSize);
        // Extra metadata for Decimal/Numeric
        if (type == TdsType.DECIMALN || type == TdsType.NUMERICN) {
          buf.put((byte) 18); // Precision (Hardcoded max for now)
          buf.put((byte) 0);  // Scale (Hardcoded default)
        }
        break;

      case USHORTLEN:
        // Writes 0xFFFF (-1) for MAX or 8000 for standard
        buf.putShort((short) 8000);
        // Collation for Strings
        if (type == TdsType.NVARCHAR || type == TdsType.BIGVARCHR) {
          writeCollation(buf);
        }
        break;

      case SCALE_LEN:
        // Time/Date2/DateTimeOffset need Scale
        buf.put((byte) 7);
        break;

      case PLP:
        // XML schema present flag (0 = none)
        buf.put((byte) 0);
        break;
    }

    // 5. Value
    writeValue(buf, type, param.value());
  }

  private void writeValue(ByteBuffer buf, TdsType type, Object value) {
    if (value == null) {
      writeNull(buf, type);
      return;
    }

    // --- Switch based on the User's Intent (R2dbcType) ---
    // This handles "Java Integer -> TinyInt" downcasting automatically
    switch (type.r2dbcType) {
      case TINYINT:
        buf.put((byte) 1); // Length
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

      case VARCHAR: // Non-Unicode
      case CHAR:
        byte[] ascii = ((String) value).getBytes(WINDOWS_1252);
        buf.putShort((short) ascii.length);
        buf.put(ascii);
        break;

      case NVARCHAR: // Unicode
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
        // Example simplified date writing (3 bytes)
        // In production, use Days from 0001-01-01
        buf.put((byte) 3);
        buf.put((byte) 0); buf.put((byte)0); buf.put((byte)0); // Placeholder
        break;

      // ... Add other types (Time, DateTime2, UUID) as needed
      default:
        throw new IllegalArgumentException("Unsupported Type for Serialization: " + type.r2dbcType);
    }
  }

  private void writeNull(ByteBuffer buf, TdsType type) {
    switch (type.strategy) {
      case BYTELEN -> buf.put((byte) 0);      // 0 length
      case USHORTLEN -> buf.putShort((short) 0xFFFF); // 0xFFFF = Null
      case PLP -> buf.putLong(0xFFFFFFFFFFFFFFFFL); // PLP Null
      default -> buf.put((byte) 0);
    }
  }

  // --- Helpers ---

  private void writeParamName(ByteBuffer buf, String name) {
    if (name == null || name.isEmpty()) {
      buf.put((byte) 0);
      return;
    }
    buf.put((byte) name.length()); // Len in chars
    buf.put(name.getBytes(StandardCharsets.UTF_16LE));
  }

  private void writeCollation(ByteBuffer buf) {
    // SQL_Latin1_General_CP1_CI_AS
    buf.put(new byte[]{0x09, 0x04, (byte) 0xD0, 0x00, 0x34});
  }

  private String buildParamDecl() {
    return params.stream()
            .map(p -> p.key().name() + " " + getSqlTypeDeclaration(p.key().type()))
            .collect(Collectors.joining(", "));
  }

  private String getSqlTypeDeclaration(TdsType type) {
    // Map Enum to SQL String (e.g. INTN -> "int")
    // You can add a field to TdsType enum for "sqlName" to make this cleaner
    if (type == TdsType.NVARCHAR) return "nvarchar(max)";
    if (type == TdsType.INTN) return "int";
    if (type == TdsType.BITN) return "bit";
    // ... simplistic fallback for now
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
}