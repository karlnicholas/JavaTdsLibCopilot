package org.tdslib.javatdslib.codec;

import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.ParameterEncoder;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Codec for encoding Date/Time values into TDS formats (DATE, TIME, DATETIME2, DATETIMEOFFSET).
 */
public class DateTimeEncoder implements ParameterEncoder {

  private static final LocalDate TDS_BASE_DATE = LocalDate.of(1, 1, 1);

  // --- Protocol Constants ---
  private static final byte SCALE_100NS = 7;
  private static final int TICKS_PER_100NS = 100;
  private static final int SECONDS_PER_MINUTE = 60;

  // --- Payload Length Constants ---
  private static final byte LEN_NULL = 0;
  private static final byte LEN_DATE = 3;
  private static final byte LEN_SMALLDATETIME = 4;
  private static final byte LEN_TIME_SCALE_7 = 5;
  private static final byte LEN_DATETIME = 8;
  private static final byte LEN_DATETIMEOFFSET = 10;

  @Override
  public boolean canEncode(TdsParameter entry) {
    TdsType type = entry.type();
    return type == TdsType.DATE || type == TdsType.TIME
        || type == TdsType.DATETIME2 || type == TdsType.DATETIMEOFFSET
        || type == TdsType.DATETIME || type == TdsType.SMALLDATETIME || type == TdsType.DATETIMN;
  }

  @Override
  public String getSqlTypeDeclaration(TdsParameter entry) {
    TdsType type = entry.type();
    if (type == TdsType.DATE) {
      return "date";
    }
    if (type == TdsType.TIME) {
      return "time(7)";
    }
    if (type == TdsType.DATETIMEOFFSET) {
      return "datetimeoffset(7)";
    }
    if (type == TdsType.DATETIME) {
      return "datetime";
    }
    if (type == TdsType.SMALLDATETIME) {
      return "smalldatetime";
    }
    return "datetime2(7)";
  }

  @Override
  public void writeTypeInfo(ByteBuffer buf, TdsParameter entry, RpcEncodingContext context) {
    TdsType type = entry.type();

    // Modern types (DATE, TIME, DATETIME2, DTOFFSET) use scale for precision
    if (type == TdsType.DATE) {
      buf.put((byte) TdsType.DATE.byteVal);
    } else if (type == TdsType.TIME) {
      buf.put((byte) TdsType.TIME.byteVal);
      buf.put(SCALE_100NS);
    } else if (type == TdsType.DATETIMEOFFSET) {
      buf.put((byte) TdsType.DATETIMEOFFSET.byteVal);
      buf.put(SCALE_100NS);
    } else if (type == TdsType.DATETIME || type == TdsType.SMALLDATETIME) {
      buf.put((byte) TdsType.DATETIMN.byteVal);
      buf.put(type == TdsType.DATETIME ? LEN_DATETIME : LEN_SMALLDATETIME);
    } else {
      buf.put((byte) TdsType.DATETIME2.byteVal);
      buf.put(SCALE_100NS);
    }
  }

  @Override
  public void writeValue(ByteBuffer buf, TdsParameter entry, RpcEncodingContext context) {
    Object value = entry.value();
    if (value == null) {
      buf.put(LEN_NULL);
      return;
    }

    TdsType type = entry.type();

    if (type == TdsType.DATE && value instanceof LocalDate) {
      buf.put(LEN_DATE);
      writeDateBytes(buf, (LocalDate) value);
    } else if (type == TdsType.TIME && value instanceof LocalTime) {
      buf.put(LEN_TIME_SCALE_7);
      writeTimeBytes(buf, (LocalTime) value);
    } else if (type == TdsType.DATETIMEOFFSET
        && (value instanceof OffsetDateTime || value instanceof java.time.ZonedDateTime)) {
      buf.put(LEN_DATETIMEOFFSET);

      OffsetDateTime odt;
      if (value instanceof java.time.ZonedDateTime zdt) {
        odt = zdt.toOffsetDateTime();
      } else {
        odt = (OffsetDateTime) value;
      }

      // FIX: MS-TDS requires the binary date/time payload to be in UTC
      OffsetDateTime utc = odt.withOffsetSameInstant(java.time.ZoneOffset.UTC);

      writeTimeBytes(buf, utc.toLocalTime());
      writeDateBytes(buf, utc.toLocalDate());

      // Write the original offset in minutes
      buf.putShort((short) (odt.getOffset().getTotalSeconds() / SECONDS_PER_MINUTE));
    } else if (value instanceof LocalDateTime) {
      buf.put(LEN_DATETIME);
      LocalDateTime ldt = (LocalDateTime) value;
      writeTimeBytes(buf, ldt.toLocalTime());
      writeDateBytes(buf, ldt.toLocalDate());
    } else if (value instanceof String) {
      // Fallback for string-based date parsing if needed
      buf.put(LEN_NULL);
    }
  }

  private void writeDateBytes(ByteBuffer buf, LocalDate date) {
    long days = ChronoUnit.DAYS.between(TDS_BASE_DATE, date);

    // DATE requires exactly 3 bytes.
    // Assuming buffer is LITTLE_ENDIAN:
    buf.put((byte) (days & 0xFF));            // Byte 0
    buf.putShort((short) (days >> 8));        // Bytes 1 and 2
  }

  private void writeTimeBytes(ByteBuffer buf, LocalTime time) {
    long ticks = time.toNanoOfDay() / TICKS_PER_100NS;

    // TIME(7) requires exactly 5 bytes.
    // Assuming buffer is LITTLE_ENDIAN:
    buf.putInt((int) ticks);                  // Bytes 0, 1, 2, 3
    buf.put((byte) (ticks >> 32));            // Byte 4
  }
}