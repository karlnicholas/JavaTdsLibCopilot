package org.tdslib.javatdslib;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.UUID;

public class TdsDataConverter {

  public static <T> T convert(byte[] data, TdsType tdsType, Class<T> type, int scale, Charset varcharCharset) {
    if (data == null) return null;

    switch (tdsType) {
      case INT1: return convertSimple(data[0], type);
      case INT2: return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort(), type);
      case INT4: return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt(), type);
      case INT8: return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong(), type);
      case INTN:
        if (data.length == 1) return convertSimple(data[0], type);
        if (data.length == 2) return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort(), type);
        if (data.length == 4) return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt(), type);
        if (data.length == 8) return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong(), type);
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
        return convertSimple(bVal ? 1 : 0, type);

      case NUMERIC:
      case DECIMAL:
      case NUMERICN:
      case DECIMALN:
        return type.cast(readDecimal(data, scale));

      case MONEY:
      case MONEYN:
      case SMALLMONEY:
        if (type == BigDecimal.class) {
          if (data.length == 4) {
            int valM = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
            return type.cast(BigDecimal.valueOf(valM, 4));
          } else {
            ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int high = bb.getInt();
            int low = bb.getInt();
            long valM = ((long) high << 32) | (low & 0xFFFFFFFFL);
            return type.cast(BigDecimal.valueOf(valM, 4));
          }
        }
        throw new UnsupportedOperationException("Money conversion to " + type.getName() + " not supported");

      case BIGCHAR:
      case BIGVARCHR:
      case VARCHAR:
      case CHAR:
      case TEXT:
        return type.cast(new String(data, varcharCharset));

      case NVARCHAR:
      case NCHAR:
      case NTEXT:
      case XML:
        return type.cast(new String(data, StandardCharsets.UTF_16LE));

      case BIGVARBIN:
      case VARBINARY:
      case BINARY:
      case IMAGE:
      case BIGBINARY:
        if (type == ByteBuffer.class) return type.cast(ByteBuffer.wrap(data));
        return type.cast(data);

      case DATE:
        int days = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8) | ((data[2] & 0xFF) << 16);
        return type.cast(LocalDate.of(1, 1, 1).plusDays(days));

      case TIME:
        return type.cast(readTime(data, scale));

      case DATETIME2:
        return type.cast(readDateTime2(data, scale));

      case DATETIMEOFFSET:
        return type.cast(readDateTimeOffset(data, scale));

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

  @SuppressWarnings("unchecked")
  private static <T> T convertSimple(long val, Class<T> type) {
    if (type == Integer.class) return (T) Integer.valueOf((int) val);
    if (type == Long.class) return (T) Long.valueOf(val);
    if (type == Short.class) return (T) Short.valueOf((short) val);
    if (type == Byte.class) return (T) Byte.valueOf((byte) val);
    if (type == Boolean.class) return (T) Boolean.valueOf(val != 0);
    return (T) Long.valueOf(val);
  }

  private static BigDecimal readDecimal(byte[] data, int scale) {
    int sign = data[0]; // 1=pos, 0=neg
    byte[] mag = new byte[data.length - 1];
    for (int i = 0; i < mag.length; i++) mag[i] = data[data.length - 1 - i];
    BigInteger bi = new BigInteger(1, mag);
    if (sign == 0) bi = bi.negate();
    return new BigDecimal(bi, scale);
  }

  private static LocalDateTime readDateTime(byte[] data) {
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

  private static LocalTime readTime(byte[] data, int scale) {
    long raw = 0;
    for (int i = 0; i < data.length; i++) raw |= ((long) (data[i] & 0xFF)) << (8 * i);
    long factor = (long) Math.pow(10, 9 - scale);
    return LocalTime.ofNanoOfDay(raw * factor);
  }

  private static LocalDateTime readDateTime2(byte[] data, int scale) {
    int timeLen = data.length - 3;
    byte[] timeBytes = new byte[timeLen];
    System.arraycopy(data, 0, timeBytes, 0, timeLen);
    LocalTime time = readTime(timeBytes, scale);

    int dayBytesStart = timeLen;
    int days = (data[dayBytesStart] & 0xFF) | ((data[dayBytesStart + 1] & 0xFF) << 8) | ((data[dayBytesStart + 2] & 0xFF) << 16);
    LocalDate date = LocalDate.of(1, 1, 1).plusDays(days);
    return LocalDateTime.of(date, time);
  }

  private static OffsetDateTime readDateTimeOffset(byte[] data, int scale) {
    int offsetBytesStart = data.length - 2;
    int dt2Len = offsetBytesStart;
    byte[] dt2Bytes = new byte[dt2Len];
    System.arraycopy(data, 0, dt2Bytes, 0, dt2Len);

    LocalDateTime utcDateTime = readDateTime2(dt2Bytes, scale);
    short offsetMinutes = ByteBuffer.wrap(data, offsetBytesStart, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
    ZoneOffset offset = ZoneOffset.ofTotalSeconds(offsetMinutes * 60);

    return OffsetDateTime.ofInstant(utcDateTime.toInstant(ZoneOffset.UTC), offset);
  }

  private static String convertBytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) sb.append(String.format("%02x", b));
    return sb.toString();
  }
}