package org.tdslib.javatdslib;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.List;
import java.util.UUID;

class TdsRowImpl implements Row {
  private static final Logger log = LoggerFactory.getLogger(TdsRowImpl.class);
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
    log.trace("TdsRowImpl::get: index: {} type: {}", index, type);
    byte[] data = columnData.get(index);
    if (data == null) return null;

    ColumnMeta meta = metadata.get(index);
    int dataType = meta.getDataType() & 0xFF;

    switch (dataType) {
      // --- Strings ---
      case TdsDataType.NVARCHAR:
      case TdsDataType.NCHAR:
      case TdsDataType.NTEXT:
        return type.cast(new String(data, StandardCharsets.UTF_16LE));

      case TdsDataType.BIGVARCHR:
      case TdsDataType.BIGCHAR:
      case TdsDataType.VARCHAR:
      case TdsDataType.CHAR:
      case TdsDataType.TEXT:
        // Simplification: Using Windows-1252 as fallback for now.
        // Real impl should check collation LCID.
        return type.cast(new String(data, Charset.forName("windows-1252")));

      // --- Integers ---
      case TdsDataType.INTN:
      case TdsDataType.INT8:
      case TdsDataType.INT4:
      case TdsDataType.INT2:
      case TdsDataType.INT1:
        long val = 0;
        // Read based on data length (which handles the N-type variations)
        if (data.length == 1) val = data[0];
        else if (data.length == 2) val = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort();
        else if (data.length == 4) val = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
        else if (data.length == 8) val = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();

        if (type == Long.class) return type.cast(val);
        if (type == Integer.class) return type.cast((int) val);
        if (type == Short.class) return type.cast((short) val);
        if (type == Byte.class) return type.cast((byte) val);
        throw new IllegalArgumentException("Cannot convert INT type to " + type.getName());

        // --- Bit ---
      case TdsDataType.BIT:
      case TdsDataType.BITN:
        boolean b = data[0] != 0;
        return type.cast(b);

      // --- Date/Time ---
// --- Date & Time ---
      case TdsDataType.DATE:
        if (type == LocalDate.class) {
          // Date is 3 bytes: Days since year 1
          int days = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8) | ((data[2] & 0xFF) << 16);
          return type.cast(LocalDate.of(1, 1, 1).plusDays(days));
        }
        break;

      case TdsDataType.TIME: // 0x29
        if (type == LocalTime.class) {
          return type.cast(readTime(data, meta.getScale() & 0xFF));
        }
        throw new UnsupportedOperationException("Time conversion to " + type.getName() + " not supported");

      case TdsDataType.DATETIME2: // 0x2A
        if (type == LocalDateTime.class) {
          return type.cast(readDateTime2(data, meta.getScale() & 0xFF));
        }
        throw new UnsupportedOperationException("DateTime2 conversion to " + type.getName() + " not supported");

      case TdsDataType.DATETIMEOFFSET:
        if (type == OffsetDateTime.class) {
          // 1. Read UTC Time from bytes
          byte[] dtData = new byte[data.length - 2];
          System.arraycopy(data, 0, dtData, 0, dtData.length);
          LocalDateTime utcDateTime = readDateTime2(dtData, meta.getScale() & 0xFF);

          // 2. Read Offset (minutes)
          int offsetMin = ByteBuffer.wrap(data, data.length - 2, 2)
                  .order(ByteOrder.LITTLE_ENDIAN).getShort();
          ZoneOffset offset = ZoneOffset.ofTotalSeconds(offsetMin * 60);

          // 3. Convert UTC Instant to Target Offset
          // Create the time as UTC, then project it to the target offset
          return type.cast(OffsetDateTime.of(utcDateTime, ZoneOffset.UTC)
                  .withOffsetSameInstant(offset));
        }
        break;
      // --- Legacy DateTime (DATETIME / SMALLDATETIME) ---
      case TdsDataType.DATETIMN: // 0x6F (Nullable)
      case TdsDataType.DATETIME: // 0x3D (Fixed 8-byte)
      case TdsDataType.DATETIM4: // 0x3A (SmallDateTime 4-byte)
        if (type == LocalDateTime.class) {
          // Base Date for Legacy Types is January 1, 1900
          LocalDate baseDate = LocalDate.of(1900, 1, 1);

          if (data.length == 8) {
            // --- Standard DATETIME (8 bytes) ---
            // Bytes 0-3: Days since 1900 (Signed Int)
            // Bytes 4-7: Ticks since midnight (1/300 of a second) (Unsigned Int)

            ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int days = bb.getInt();
            int ticks = bb.getInt(); // Read as signed int, but practically always positive

            LocalDate date = baseDate.plusDays(days);

            // Convert 1/300s ticks to nanoseconds
            // 1 tick = 3.333333 ms
            // Formula: ticks * 1000 / 300 = millis
            long millis = Math.round(ticks * 1000.0 / 300.0);
            LocalTime time = LocalTime.ofNanoOfDay(millis * 1_000_000);

            return type.cast(LocalDateTime.of(date, time));

          } else if (data.length == 4) {
            // --- SMALLDATETIME (4 bytes) ---
            // Bytes 0-1: Days since 1900 (Unsigned Short)
            // Bytes 2-3: Minutes since midnight (Unsigned Short)

            ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int days = bb.getShort() & 0xFFFF;
            int minutes = bb.getShort() & 0xFFFF;

            LocalDate date = baseDate.plusDays(days);
            LocalTime time = LocalTime.ofSecondOfDay(minutes * 60L);

            return type.cast(LocalDateTime.of(date, time));
          }
        }
        throw new UnsupportedOperationException("DateTime conversion to " + type.getName() + " not supported for length " + data.length);
      // --- Floating Point (Real/Float) ---
      case TdsDataType.FLT4:
      case TdsDataType.FLT8:
      case TdsDataType.FLTN:
        if (data.length == 4) {
          // It is a REAL (4 bytes)
          float valF = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getFloat();

          if (type == Float.class) return type.cast(valF);
          if (type == Double.class) return type.cast((double) valF); // Promote to Double

        } else if (data.length == 8) {
          // It is a FLOAT (8 bytes)
          double valF = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getDouble();

          if (type == Double.class) return type.cast(valF);
          if (type == Float.class) return type.cast((float) valF); // Narrow to Float (potential precision loss)
        }
        throw new UnsupportedOperationException("Float conversion to " + type.getName() + " not supported or invalid data length");
      // --- Binary ---
      case TdsDataType.BIGVARBIN:
      case TdsDataType.BIGBINARY:
      case TdsDataType.VARBINARY:
      case TdsDataType.BINARY:
      case TdsDataType.IMAGE:
        if (type == byte[].class) return type.cast(data);
        throw new UnsupportedOperationException("Binary conversion to " + type.getName() + " not supported");

      // --- Not Implemented Stubs ---
      // ... inside TdsRowImpl.get() ...

// --- Money ---
      case TdsDataType.MONEY:
      case TdsDataType.MONEYN:
      case TdsDataType.MONEY4:
        if (type == BigDecimal.class) {
          if (data.length == 4) {
            // SmallMoney: Standard 4-byte Little Endian int
            int valM = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
            return type.cast(BigDecimal.valueOf(valM, 4));
          } else {
            // Money: 8 bytes, split into [High 4 bytes] + [Low 4 bytes]
            ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int high = bb.getInt(); // Read first 4 bytes
            int low = bb.getInt();  // Read next 4 bytes

            // Combine them: (High << 32) | Unsigned Low
            long valM = ((long) high << 32) | (low & 0xFFFFFFFFL);

            return type.cast(BigDecimal.valueOf(valM, 4));
          }
        }
        throw new UnsupportedOperationException("Money conversion to " + type.getName() + " not supported");      case TdsDataType.NUMERIC:

      case TdsDataType.NUMERICN:
      case TdsDataType.DECIMAL:
      case TdsDataType.DECIMALN:
        if (type == BigDecimal.class) {
          // Data format: [Sign] [Byte1] [Byte2] ... [ByteN]
          // Sign: 1 = Positive, 0 = Negative
          // Bytes 1..N: Unscaled value in Little Endian

          byte sign = data[0];

          // Reverse bytes 1..N to get Big Endian (Java default for BigInteger)
          byte[] magnitude = new byte[data.length - 1];
          for (int i = 0; i < magnitude.length; i++) {
            magnitude[i] = data[data.length - 1 - i];
          }

          // Create BigInteger (always positive initially)
          BigInteger bi = new BigInteger(1, magnitude);

          // Apply Sign
          if (sign == 0) {
            bi = bi.negate();
          }

          // Create BigDecimal using the Scale from Metadata
          int scale = meta.getScale() & 0xFF; // Scale is stored in ColumnMeta
          return type.cast(new BigDecimal(bi, scale));
        }
        throw new UnsupportedOperationException("Decimal conversion to " + type.getName() + " not supported");
      case TdsDataType.GUID:
        if (data == null) return null;
        if (type == UUID.class || type == String.class) {
          ByteBuffer bb = ByteBuffer.wrap(data);

          // SQL Server GUID: Int(LE) - Short(LE) - Short(LE) - Byte[8]
          // We read them as Big Endian (Java default) and reverse bytes
          long part1 = Integer.reverseBytes(bb.getInt()) & 0xFFFFFFFFL;
          long part2 = Short.reverseBytes(bb.getShort()) & 0xFFFFL;
          long part3 = Short.reverseBytes(bb.getShort()) & 0xFFFFL;

          // Combine MSB
          long msb = (part1 << 32) | (part2 << 16) | part3;

          // LSB is stored sequentially (Big Endian equivalent for byte array)
          long lsb = bb.getLong();

          UUID uuid = new UUID(msb, lsb);

          if (type == UUID.class) return type.cast(uuid);
          if (type == String.class) return type.cast(uuid.toString());
        }
        throw new UnsupportedOperationException("GUID conversion to " + type.getName() + " not supported");
      case TdsDataType.XML:
        if (type == String.class) {
          // XML is transmitted as binary UTF-16LE.
          // The byte[] data has already been fully reassembled by the RowTokenParser.
          return type.cast(new String(data, StandardCharsets.UTF_16LE));
        }
        throw new UnsupportedOperationException("XML conversion to " + type.getName() + " not supported");
      default:
        throw new UnsupportedOperationException("Unknown DataType: 0x" + Integer.toHexString(dataType));
    }
    return null;
  }

  // Helper for DateTime2
  private <T> T parseDateTime2(byte[] data, Class<T> type) {
    // Reusing logic from your previous snippet
    if (data.length == 7) {
      long millis = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8) | ((data[2] & 0xFF) << 16) | ((long)(data[3] & 0xFF) << 24);
      int days = (data[4] & 0xFF) | ((data[5] & 0xFF) << 8) | ((data[6] & 0xFF) << 16);
      LocalDate date = LocalDate.of(1, 1, 1).plusDays(days);
      LocalTime time = LocalTime.ofNanoOfDay(millis * 1_000_000);
      return type.cast(LocalDateTime.of(date, time));
    }
    throw new UnsupportedOperationException("Only DATETIME2(3) supported in stub");
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    for (int i = 0; i < metadata.size(); i++) {
      if (metadata.get(i).getName().equalsIgnoreCase(name)) {
        return get(i, type);
      }
    }
    throw new IllegalArgumentException("Column not found: " + name);
  }
  private LocalTime readTime(byte[] data, int scale) {
    // Read variable-length unsigned integer (Little Endian)
    long rawTime = 0;
    for (int i = 0; i < data.length; i++) {
      rawTime |= ((long)(data[i] & 0xFF)) << (8 * i);
    }

    // Calculate nanoseconds. Unit is 10^-scale seconds.
    // E.g., Scale 7 = 100ns units. Factor = 100.
    long factor = (long) Math.pow(10, 9 - scale);
    long nanos = rawTime * factor;

    return LocalTime.ofNanoOfDay(nanos);
  }

  private LocalDateTime readDateTime2(byte[] data, int scale) {
    // DATETIME2 = Time (Var Bytes) + Date (3 Bytes)
    int timeByteLen = data.length - 3;

    // 1. Parse Time
    byte[] timeBytes = new byte[timeByteLen];
    System.arraycopy(data, 0, timeBytes, 0, timeByteLen);
    LocalTime time = readTime(timeBytes, scale);

    // 2. Parse Date (Last 3 bytes)
    int dateOffset = timeByteLen;
    int days = (data[dateOffset] & 0xFF) |
            ((data[dateOffset+1] & 0xFF) << 8) |
            ((data[dateOffset+2] & 0xFF) << 16);

    LocalDate date = LocalDate.of(1, 1, 1).plusDays(days);

    return LocalDateTime.of(date, time);
  }
}