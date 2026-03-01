package org.tdslib.javatdslib.codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.tdslib.javatdslib.protocol.TdsType;

/**
 * Decodes TDS date and time types into Java Time objects.
 */
public class DateTimeDecoder implements ResultDecoder {

  // Lookup array replacing Math.pow() for instant time scaling
  private static final long[] NANOS_FACTOR = {
      1000000000L, 100000000L, 10000000L, 1000000L, 100000L, 10000L, 1000L, 100L
  };

  @Override
  public boolean canDecode(TdsType tdsType) {
    return tdsType == TdsType.DATE
        || tdsType == TdsType.TIME
        || tdsType == TdsType.DATETIME2
        || tdsType == TdsType.DATETIMEOFFSET
        || tdsType == TdsType.DATETIMN
        || tdsType == TdsType.DATETIME
        || tdsType == TdsType.SMALLDATETIME;
  }

  @Override
  public <T> T decode(byte[] data, TdsType tdsType, Class<T> targetType, int scale,
                      Charset varcharCharset) {
    Object result;
    switch (tdsType) {
      case DATE:
        int days = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8) | ((data[2] & 0xFF) << 16);
        result = LocalDate.of(1, 1, 1).plusDays(days);
        break;
      case TIME:
        result = readTime(data, scale);
        break;
      case DATETIME2:
        result = readDateTime2(data, scale);
        break;
      case DATETIMEOFFSET:
        result = readDateTimeOffset(data, scale);
        break;
      case DATETIMN:
      case DATETIME:
      case SMALLDATETIME:
        result = readDateTime(data);
        break;
      default:
        throw new IllegalStateException("Unexpected type: " + tdsType);
    }
    return targetType.cast(result);
  }

  private LocalTime readTime(byte[] data, int scale) {
    long raw = 0;
    for (int i = 0; i < data.length; i++) {
      raw |= ((long) (data[i] & 0xFF)) << (8 * i);
    }
    // FIX: Replaced Math.pow with highly performant array lookup
    long factor = NANOS_FACTOR[scale];
    return LocalTime.ofNanoOfDay(raw * factor);
  }

  private LocalDateTime readDateTime2(byte[] data, int scale) {
    int timeLen = data.length - 3;
    byte[] timeBytes = new byte[timeLen];
    System.arraycopy(data, 0, timeBytes, 0, timeLen);
    LocalTime time = readTime(timeBytes, scale);

    int dayBytesStart = timeLen;
    int days = (data[dayBytesStart] & 0xFF) | ((data[dayBytesStart + 1] & 0xFF) << 8)
        | ((data[dayBytesStart + 2] & 0xFF) << 16);
    LocalDate date = LocalDate.of(1, 1, 1).plusDays(days);
    return LocalDateTime.of(date, time);
  }

  private OffsetDateTime readDateTimeOffset(byte[] data, int scale) {
    int offsetBytesStart = data.length - 2;
    int dt2Len = offsetBytesStart;
    byte[] dt2Bytes = new byte[dt2Len];
    System.arraycopy(data, 0, dt2Bytes, 0, dt2Len);

    LocalDateTime utcDateTime = readDateTime2(dt2Bytes, scale);
    short offsetMinutes = ByteBuffer.wrap(data, offsetBytesStart, 2)
        .order(ByteOrder.LITTLE_ENDIAN).getShort();
    ZoneOffset offset = ZoneOffset.ofTotalSeconds(offsetMinutes * 60);

    return OffsetDateTime.ofInstant(utcDateTime.toInstant(ZoneOffset.UTC), offset);
  }

  private LocalDateTime readDateTime(byte[] data) {
    LocalDate baseDate = LocalDate.of(1900, 1, 1);

    if (data.length == 8) {
      int days = ByteBuffer.wrap(data, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      long ticks = Integer.toUnsignedLong(ByteBuffer.wrap(data, 4, 4)
          .order(ByteOrder.LITTLE_ENDIAN).getInt());
      // FIX: Removed dangerous Double Math to prevent IEEE-754 precision loss
      long nanos = (ticks * 10000000L) / 3L;
      return LocalDateTime.of(baseDate.plusDays(days), LocalTime.ofNanoOfDay(nanos));
    } else if (data.length == 4) {
      int days = ByteBuffer.wrap(data, 0, 2).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
      int minutes = ByteBuffer.wrap(data, 2, 2).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
      return LocalDateTime.of(baseDate.plusDays(days), LocalTime.of(0, 0).plusMinutes(minutes));
    }
    throw new IllegalStateException("Invalid data length for DateTime: " + data.length);
  }
}