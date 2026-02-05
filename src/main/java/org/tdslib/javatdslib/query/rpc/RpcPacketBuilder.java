package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.headers.AllHeaders;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public class RpcPacketBuilder {

  private static final short RPC_PROCID_SWITCH = (short) 0xFFFF;
  private static final short RPC_PROCID_SPEXECUTESQL = 10;
  private static final byte RPC_PARAM_DEFAULT = 0x00;

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
    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024).order(ByteOrder.LITTLE_ENDIAN);

    buf.putShort(RPC_PROCID_SWITCH);
    buf.putShort(RPC_PROCID_SPEXECUTESQL);
    buf.putShort((short) 0x0000);

    putParamName(buf, "@stmt");
    buf.put(RPC_PARAM_DEFAULT);
    putTypeInfoNVarcharMax(buf);
    putPlpUnicodeString(buf, sql);

    if (!params.isEmpty()) {
      String paramsDecl = buildParamsDeclaration();
      putParamName(buf, "@params");
      buf.put(RPC_PARAM_DEFAULT);
      putTypeInfoNVarcharMax(buf);
      putPlpUnicodeString(buf, paramsDecl);

      for (ParamEntry entry : params) {
        putParamName(buf, entry.key().name());
        buf.put(RPC_PARAM_DEFAULT);
        boolean forcePlp = isLargeString(entry);
        putTypeInfoForParam(buf, entry, forcePlp);
        putParamValue(buf, entry, forcePlp);
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

  private boolean isLargeString(ParamEntry entry) {
    if (entry.value() instanceof String s) {
      return s.length() > 4000;
    }
    return false;
  }

  private String buildParamsDeclaration() {
    if (params.isEmpty()) return "";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < params.size(); i++) {
      ParamEntry entry = params.get(i);
      if (i > 0) sb.append(", ");
      String typeName = entry.key().type().getSqlTypeName();
      if (isLargeString(entry)) typeName = "nvarchar(max)";
      sb.append(entry.key().name()).append(" ").append(typeName);
    }
    return sb.toString();
  }

  private void putParamName(ByteBuffer buf, String name) {
    byte[] bytes = name.getBytes(StandardCharsets.UTF_16LE);
    buf.put((byte) (bytes.length / 2));
    buf.put(bytes);
  }

  private void putTypeInfoNVarcharMax(ByteBuffer buf) {
    buf.put((byte) 0xE7);
    buf.putShort((short) -1);
    buf.putInt(0x00000409);
    buf.put((byte) 0x00);
  }

  private void putPlpUnicodeString(ByteBuffer buf, String str) {
    if (str == null) {
      buf.putLong(0xFFFFFFFFFFFFFFFFL);
      return;
    }
    byte[] bytes = str.getBytes(StandardCharsets.UTF_16LE);
    buf.putLong(bytes.length);
    if (bytes.length > 0) {
      buf.putInt(bytes.length);
      buf.put(bytes);
    }
    buf.putInt(0);
  }

  private void putTypeInfoForParam(ByteBuffer buf, ParamEntry entry, boolean forcePlp) {
    BindingType type = entry.key().type();
    if (forcePlp) {
      putTypeInfoNVarcharMax(buf);
      return;
    }

    buf.put(type.getTdsXType());

    byte precision = (type.getPrecision() != null) ? type.getPrecision() : 0;
    byte scale = (type.getScale() != null) ? type.getScale() : 0;
    if (entry.value() instanceof BigDecimal bd) {
      int p = bd.precision();
      int s = bd.scale();
      if (p > 38) p = 38; if (s < 0) s = 0; if (s > p) s = p;
      precision = (byte) p; scale = (byte) s;
    }

    switch (type.getTypeStyle()) {
      case FIXED -> {
        // [FIX 1] UNIQUEIDENTIFIER (0x24) needs MaxLen (0x10)
        if (type == BindingType.UNIQUEIDENTIFIER) {
          buf.put((byte) 0x10);
        }
        // [FIX 2] TIME/DT2/DTOFFSET need Scale
        else if (type == BindingType.TIME || type == BindingType.DATETIME2 || type == BindingType.DATETIMEOFFSET) {
          buf.put((byte) 0x07); // Scale 7
        }
      }
      case LENGTH -> {
        if (type.getLength() == null) throw new IllegalStateException("Missing length");
        buf.put(type.getLength().byteValue());
      }
      case PREC_SCALE -> {
        buf.put(getDecimalStorageLength(precision));
        buf.put(precision);
        buf.put(scale);
      }
      case VARLEN -> {
        Number maxLen = type.getLength();
        if (maxLen.intValue() == -1) buf.putShort((short) -1);
        else buf.putShort(maxLen.shortValue());

        if (type == BindingType.CHAR || type == BindingType.VARCHAR ||
            type == BindingType.NCHAR || type == BindingType.NVARCHAR ||
            type == BindingType.TEXT  || type == BindingType.NTEXT) {
          buf.putInt(0x00000409);
          buf.put((byte) 0x00);
        }
      }
    }
  }

  private void putParamValue(ByteBuffer buf, ParamEntry entry, boolean forcePlp) {
    if (forcePlp) {
      putPlpUnicodeString(buf, (String) entry.value());
      return;
    }

    BindingType type = entry.key().type();
    Object value = entry.value();

    byte precision = (type.getPrecision() != null) ? type.getPrecision() : 0;
    byte scale = (type.getScale() != null) ? type.getScale() : 0;
    if (value instanceof BigDecimal bd) {
      int p = bd.precision(); int s = bd.scale();
      if (p > 38) p = 38; if (s < 0) s = 0; if (s > p) s = p;
      precision = (byte) p; scale = (byte) s;
    }

    if (value == null) {
      switch (type.getTypeStyle()) {
        case FIXED -> buf.put((byte) 0x00);
        case LENGTH, VARLEN, PREC_SCALE -> buf.put((byte) 0xFF);
      }
      return;
    }

    switch (type.getTypeStyle()) {
      case FIXED -> writeFixedValue(buf, type, value);
      case LENGTH -> {
        buf.put(type.getLength().byteValue());
        writeFixedLengthValue(buf, type, value);
      }
      case PREC_SCALE -> {
        byte[] dBytes = convertToDecimalBytes(value, precision, scale);
        buf.put((byte) dBytes.length);
        buf.put(dBytes);
      }
      case VARLEN -> {
        byte[] bytes = convertToVarlenBytes(type, value);
        if (type.getLength().intValue() == -1) {
          buf.putLong(bytes.length);
          if (bytes.length > 0) {
            buf.putInt(bytes.length);
            buf.put(bytes);
          }
          buf.putInt(0);
        } else {
          buf.putShort((short) bytes.length);
          buf.put(bytes);
        }
      }
    }
  }

  private void writeFixedValue(ByteBuffer buf, BindingType type, Object value) {
    if (type == BindingType.TIME) {
      LocalTime t = (LocalTime) value;
      long ticks = t.toNanoOfDay() / 100;
      buf.put((byte) 5); // Len
      buf.put((byte) (ticks & 0xFF));
      buf.put((byte) ((ticks >> 8) & 0xFF));
      buf.put((byte) ((ticks >> 16) & 0xFF));
      buf.put((byte) ((ticks >> 24) & 0xFF));
      buf.put((byte) ((ticks >> 32) & 0xFF));
    } else if (type == BindingType.DATETIMEOFFSET) {
      OffsetDateTime odt = (OffsetDateTime) value;
      LocalTime t = odt.toLocalTime();
      long ticks = t.toNanoOfDay() / 100;
      long days = odt.toLocalDate().toEpochDay() + DAYS_TO_1970;
      short offsetMin = (short) (odt.getOffset().getTotalSeconds() / 60);

      buf.put((byte) 10); // Len
      buf.put((byte) (ticks & 0xFF));
      buf.put((byte) ((ticks >> 8) & 0xFF));
      buf.put((byte) ((ticks >> 16) & 0xFF));
      buf.put((byte) ((ticks >> 24) & 0xFF));
      buf.put((byte) ((ticks >> 32) & 0xFF));
      buf.put((byte) (days & 0xFF));
      buf.put((byte) ((days >> 8) & 0xFF));
      buf.put((byte) ((days >> 16) & 0xFF));
      buf.putShort(offsetMin);
    } else if (type == BindingType.DATETIME2) {
      LocalDateTime dt = (LocalDateTime) value;
      LocalTime t = dt.toLocalTime();
      long ticks = t.toNanoOfDay() / 100;
      long days = dt.toLocalDate().toEpochDay() + DAYS_TO_1970;
      buf.put((byte) 8); // Len
      buf.put((byte) (ticks & 0xFF));
      buf.put((byte) ((ticks >> 8) & 0xFF));
      buf.put((byte) ((ticks >> 16) & 0xFF));
      buf.put((byte) ((ticks >> 24) & 0xFF));
      buf.put((byte) ((ticks >> 32) & 0xFF));
      buf.put((byte) (days & 0xFF));
      buf.put((byte) ((days >> 8) & 0xFF));
      buf.put((byte) ((days >> 16) & 0xFF));
    } else if (type == BindingType.DATE) {
      LocalDate d = (LocalDate) value;
      long days = d.toEpochDay() + DAYS_TO_1970;
      buf.put((byte) 3); // Len
      buf.put((byte) (days & 0xFF));
      buf.put((byte) ((days >> 8) & 0xFF));
      buf.put((byte) ((days >> 16) & 0xFF));
    } else if (type == BindingType.UNIQUEIDENTIFIER) {
      UUID uuid = (UUID) value;
      // [FIX 3] Write Length (16)
      buf.put((byte) 0x10);
      buf.putLong(uuid.getMostSignificantBits());
      buf.putLong(uuid.getLeastSignificantBits());
    } else {
      writeFixedLengthValue(buf, type, value);
    }
  }

  private void writeFixedLengthValue(ByteBuffer buf, BindingType type, Object value) {
    if (value instanceof Integer) buf.putInt((Integer) value);
    else if (value instanceof Long) buf.putLong((Long) value);
    else if (value instanceof Short) buf.putShort((Short) value);
    else if (value instanceof Byte) buf.put((Byte) value);
    else if (value instanceof Float) buf.putFloat((Float) value);
    else if (value instanceof Double) buf.putDouble((Double) value);
    else if (value instanceof Boolean) buf.put((byte) ((Boolean) value ? 1 : 0));
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

  private byte[] convertToVarlenBytes(BindingType type, Object value) {
    if (value instanceof String s) return s.getBytes(StandardCharsets.UTF_16LE);
    else if (value instanceof byte[] b) return b;
    else if (value instanceof ByteBuffer bb) {
      byte[] b = new byte[bb.remaining()];
      bb.duplicate().get(b);
      return b;
    }
    throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to varlen bytes");
  }

  private byte getDecimalStorageLength(byte precision) {
    if (precision <= 9) return 5;
    if (precision <= 19) return 9;
    if (precision <= 28) return 13;
    return 17;
  }
}