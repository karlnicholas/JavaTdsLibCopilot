package org.tdslib.javatdslib.codec;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.rpc.ParamEntry;
import org.tdslib.javatdslib.rpc.ParameterCodec;
import org.tdslib.javatdslib.rpc.RpcEncodingContext;

/**
 * Codec for encoding Date/Time values into TDS formats (DATE, TIME, DATETIME2, DATETIMEOFFSET).
 */
public class DateTimeEncoder implements ParameterCodec {

  private static final LocalDate TDS_BASE_DATE = LocalDate.of(1, 1, 1);

  @Override
  public boolean canEncode(ParamEntry entry) {
    TdsType type = entry.key().type();
    return type == TdsType.DATE || type == TdsType.TIME
        || type == TdsType.DATETIME2 || type == TdsType.DATETIMEOFFSET
        || type == TdsType.DATETIME || type == TdsType.SMALLDATETIME || type == TdsType.DATETIMN;
  }

  @Override
  public String getSqlTypeDeclaration(ParamEntry entry) {
    TdsType type = entry.key().type();
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
  public void writeTypeInfo(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    TdsType type = entry.key().type();

    // Modern types (DATE, TIME, DATETIME2, DTOFFSET) use scale for precision
    if (type == TdsType.DATE) {
      buf.put((byte) TdsType.DATE.byteVal);
    } else if (type == TdsType.TIME) {
      buf.put((byte) TdsType.TIME.byteVal);
      buf.put((byte) 7); // Scale 7 (100ns precision)
    } else if (type == TdsType.DATETIMEOFFSET) {
      buf.put((byte) TdsType.DATETIMEOFFSET.byteVal);
      buf.put((byte) 7); // Scale 7
    } else if (type == TdsType.DATETIME || type == TdsType.SMALLDATETIME) {
      buf.put((byte) TdsType.DATETIMN.byteVal);
      buf.put((byte) (type == TdsType.DATETIME ? 8 : 4));
    } else {
      buf.put((byte) TdsType.DATETIME2.byteVal);
      buf.put((byte) 7); // Scale 7
    }
  }

  @Override
  public void writeValue(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    Object value = entry.value().getValue();
    if (value == null) {
      buf.put((byte) 0);
      return;
    }

    TdsType type = entry.key().type();

    if (type == TdsType.DATE && value instanceof LocalDate) {
      buf.put((byte) 3);
      writeDateBytes(buf, (LocalDate) value);
    } else if (type == TdsType.TIME && value instanceof LocalTime) {
      buf.put((byte) 5);
      writeTimeBytes(buf, (LocalTime) value);
    } else if (type == TdsType.DATETIMEOFFSET && value instanceof OffsetDateTime) {
      buf.put((byte) 10);
      OffsetDateTime odt = (OffsetDateTime) value;

      // FIX: MS-TDS requires the binary date/time payload to be in UTC
      OffsetDateTime utc = odt.withOffsetSameInstant(java.time.ZoneOffset.UTC);

      writeTimeBytes(buf, utc.toLocalTime());
      writeDateBytes(buf, utc.toLocalDate());

      // Write the original offset in minutes
      buf.putShort((short) (odt.getOffset().getTotalSeconds() / 60));
    } else if (value instanceof LocalDateTime) {
      buf.put((byte) 8);
      LocalDateTime ldt = (LocalDateTime) value;
      writeTimeBytes(buf, ldt.toLocalTime());
      writeDateBytes(buf, ldt.toLocalDate());
    } else if (value instanceof String) {
      // Fallback for string-based date parsing if needed
      buf.put((byte) 0);
    }
  }

  private void writeDateBytes(ByteBuffer buf, LocalDate date) {
    long days = ChronoUnit.DAYS.between(TDS_BASE_DATE, date);
    buf.put((byte) (days & 0xFF));
    buf.put((byte) ((days >> 8) & 0xFF));
    buf.put((byte) ((days >> 16) & 0xFF));
  }

  private void writeTimeBytes(ByteBuffer buf, LocalTime time) {
    long ticks = time.toNanoOfDay() / 100; // 100ns increments for scale 7
    buf.put((byte) (ticks & 0xFF));
    buf.put((byte) ((ticks >> 8) & 0xFF));
    buf.put((byte) ((ticks >> 16) & 0xFF));
    buf.put((byte) ((ticks >> 24) & 0xFF));
    buf.put((byte) ((ticks >> 32) & 0xFF));
  }
}