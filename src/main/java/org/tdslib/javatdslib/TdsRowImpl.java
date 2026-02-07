package org.tdslib.javatdslib;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.List;
import java.util.UUID;

class TdsRowImpl implements Row {
  private final List<byte[]> columnData;
  private final List<ColumnMeta> metadata;
  private final TdsRowMetadataImpl rowMetadata;

  TdsRowImpl(List<byte[]> columnData, List<ColumnMeta> metadata) {
    this.columnData = columnData;
    this.metadata = metadata;
    this.rowMetadata = new TdsRowMetadataImpl(metadata);
  }

  @Override
  public RowMetadata getMetadata() {
    return this.rowMetadata;
  }

  @Override
  public <T> T get(int index, Class<T> type) {
    if (index < 0 || index >= columnData.size()) {
      throw new IllegalArgumentException("Invalid Column Index: " + index);
    }

    byte[] data = columnData.get(index);
    ColumnMeta meta = metadata.get(index);

    if (data == null) return null;

    TdsType tdsType = TdsType.valueOf((byte) meta.getDataType());
    if (tdsType == null) {
      throw new IllegalStateException("Unknown TDS Type: " + meta.getDataType());
    }

    switch (tdsType) {
      case INT1: return convert(data[0], type);
      case INT2: return convert(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort(), type);
      case INT4: return convert(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt(), type);
      case INT8: return convert(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong(), type);
      case INTN:
        if (data.length == 1) return convert(data[0], type);
        if (data.length == 2) return convert(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort(), type);
        if (data.length == 4) return convert(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt(), type);
        if (data.length == 8) return convert(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong(), type);
        throw new IllegalStateException("Unexpected INTN length");

      case FLT4: return type.cast(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getFloat());

      case FLTN:
        if (data.length == 4) {
          return type.cast(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getFloat());
        }
        // Fallthrough if length is 8
      case FLT8:
        double dVal = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getDouble();
        if (type == Float.class) return type.cast((float) dVal);
        return type.cast(dVal);

      case BIT:
      case BITN:
        boolean bVal = data[0] != 0;
        if (type == Boolean.class) return type.cast(bVal);
        return convert(bVal ? 1 : 0, type);

      case NUMERIC:
      case DECIMAL:
      case NUMERICN:
      case DECIMALN:
      case MONEY:
      case SMALLMONEY:
      case MONEYN:
        return type.cast(readDecimal(data, meta.getScale()));

      // FIX: Added BIGCHAR to this block
      case BIGCHAR:
      case BIGVARCHR:
      case VARCHAR:
      case CHAR:
      case TEXT:
        // These are single-byte charsets (usually Windows-1252 or ASCII)
        return type.cast(new String(data, java.nio.charset.Charset.forName("windows-1252")));

      case NVARCHAR:
      case NCHAR:
      case NTEXT:
      case XML:
        // These are UTF-16LE
        return type.cast(new String(data, StandardCharsets.UTF_16LE));

      case BIGVARBIN:
      case VARBINARY:
      case BINARY:
      case IMAGE:
        // FIX: Added BIGBINARY support just in case
      case BIGBINARY:
        if (type == ByteBuffer.class) return type.cast(ByteBuffer.wrap(data));
        return type.cast(data);

      case DATE:
        int days = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8) | ((data[2] & 0xFF) << 16);
        return type.cast(LocalDate.of(1, 1, 1).plusDays(days));

      case TIME:
        return type.cast(readTime(data, meta.getScale()));

      case DATETIME2:
        return type.cast(readDateTime2(data, meta.getScale()));

      case DATETIMEOFFSET:
        return type.cast(readDateTimeOffset(data, meta.getScale()));

      case DATETIMN:
      case DATETIME:
      case SMALLDATETIME:
        return type.cast(readDateTime(data));

      case GUID:
        if (type == UUID.class) {
          ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
          long msb = bb.getLong();
          long lsb = bb.getLong();
          return type.cast(new UUID(msb, lsb));
        }
        return type.cast(convertBytesToHex(data));

      default:
        throw new UnsupportedOperationException("Type conversion not implemented for: " + tdsType);
    }
  }

  // --- Helpers ---
  @SuppressWarnings("unchecked")
  private <T> T convert(long val, Class<T> type) {
    if (type == Integer.class) return (T) Integer.valueOf((int) val);
    if (type == Long.class) return (T) Long.valueOf(val);
    if (type == Short.class) return (T) Short.valueOf((short) val);
    if (type == Byte.class) return (T) Byte.valueOf((byte) val);
    if (type == Boolean.class) return (T) Boolean.valueOf(val != 0);
    return (T) Long.valueOf(val);
  }

  private BigDecimal readDecimal(byte[] data, int scale) {
    int sign = data[0]; // 1=pos, 0=neg
    byte[] mag = new byte[data.length - 1];
    for(int i=0; i<mag.length; i++) mag[i] = data[data.length - 1 - i];
    BigInteger bi = new BigInteger(1, mag);
    if (sign == 0) bi = bi.negate();
    return new BigDecimal(bi, scale);
  }

  private LocalDateTime readDateTime(byte[] data) {
    LocalDate baseDate = LocalDate.of(1900, 1, 1);

    if (data.length == 8) {
      int days = ByteBuffer.wrap(data, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      int ticks = ByteBuffer.wrap(data, 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();

      LocalDate date = baseDate.plusDays(days);
      long nanos = (long) ((ticks * 1000000000.0) / 300.0);
      LocalTime time = LocalTime.ofNanoOfDay(nanos);

      return LocalDateTime.of(date, time);
    } else if (data.length == 4) {
      int days = ByteBuffer.wrap(data, 0, 2).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
      int minutes = ByteBuffer.wrap(data, 2, 2).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;

      LocalDate date = baseDate.plusDays(days);
      LocalTime time = LocalTime.of(0, 0).plusMinutes(minutes);

      return LocalDateTime.of(date, time);
    }
    throw new IllegalStateException("Invalid data length for DateTime: " + data.length);
  }

  private LocalTime readTime(byte[] data, int scale) {
    long raw = 0;
    for (int i=0; i<data.length; i++) raw |= ((long)(data[i] & 0xFF)) << (8*i);
    long factor = (long) Math.pow(10, 9 - scale);
    return LocalTime.ofNanoOfDay(raw * factor);
  }

  private LocalDateTime readDateTime2(byte[] data, int scale) {
    int timeLen = data.length - 3;
    byte[] timeBytes = new byte[timeLen];
    System.arraycopy(data, 0, timeBytes, 0, timeLen);
    LocalTime time = readTime(timeBytes, scale);

    int dayBytesStart = timeLen;
    int days = (data[dayBytesStart] & 0xFF) | ((data[dayBytesStart+1] & 0xFF) << 8) | ((data[dayBytesStart+2] & 0xFF) << 16);
    LocalDate date = LocalDate.of(1, 1, 1).plusDays(days);
    return LocalDateTime.of(date, time);
  }

  private OffsetDateTime readDateTimeOffset(byte[] data, int scale) {
    int offsetBytesStart = data.length - 2;
    int dt2Len = offsetBytesStart;
    byte[] dt2Bytes = new byte[dt2Len];
    System.arraycopy(data, 0, dt2Bytes, 0, dt2Len);
    LocalDateTime ldt = readDateTime2(dt2Bytes, scale);

    short offsetMinutes = ByteBuffer.wrap(data, offsetBytesStart, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
    return OffsetDateTime.of(ldt, ZoneOffset.ofTotalSeconds(offsetMinutes * 60));
  }

  private String convertBytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) sb.append(String.format("%02x", b));
    return sb.toString();
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    for (int i = 0; i < metadata.size(); i++) {
      if (metadata.get(i).getName().equalsIgnoreCase(name)) return get(i, type);
    }
    throw new IllegalArgumentException("Column not found: " + name);
  }
}