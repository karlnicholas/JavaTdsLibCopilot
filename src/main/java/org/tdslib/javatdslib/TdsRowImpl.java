package org.tdslib.javatdslib;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

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
      case TdsDataType.DATE:
        long days = ((data[2] & 0xFFL) << 16) | ((data[1] & 0xFFL) << 8) | (data[0] & 0xFFL);
        return type.cast(LocalDate.of(1, 1, 1).plusDays(days));

      case TdsDataType.DATETIME2:
        // Simplified parser for example (assuming scale 3 or 7 structure)
        // See previous impl for full logic
        return parseDateTime2(data, type);

      case TdsDataType.DATETIME:
      case TdsDataType.DATETIMN:
        // TODO: Implement DateTime 8-byte parsing
        throw new UnsupportedOperationException("DATETIME (Classic) not implemented yet");

        // --- Floats ---
      case TdsDataType.FLTN:
      case TdsDataType.FLT8:
        return type.cast(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getDouble());
      case TdsDataType.FLT4:
        return type.cast(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getFloat());

      // --- Binary ---
      case TdsDataType.BIGVARBIN:
      case TdsDataType.VARBINARY:
      case TdsDataType.BINARY:
      case TdsDataType.IMAGE:
        if (type == byte[].class) return type.cast(data);
        throw new UnsupportedOperationException("Binary conversion to " + type.getName() + " not supported");

        // --- Not Implemented Stubs ---
      case TdsDataType.MONEY:
      case TdsDataType.MONEY4:
      case TdsDataType.MONEYN:
        throw new UnsupportedOperationException("MONEY types not implemented");

      case TdsDataType.NUMERIC:
      case TdsDataType.NUMERICN:
      case TdsDataType.DECIMAL:
      case TdsDataType.DECIMALN:
        throw new UnsupportedOperationException("DECIMAL/NUMERIC types not implemented");

      case TdsDataType.GUID:
        throw new UnsupportedOperationException("GUID type not implemented");

      case TdsDataType.XML:
        throw new UnsupportedOperationException("XML type not implemented");

      default:
        throw new UnsupportedOperationException("Unknown DataType: 0x" + Integer.toHexString(dataType));
    }
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
}