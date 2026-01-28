package org.tdslib.javatdslib;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

class TdsRow implements Row {
  private final List<byte[]> columnData;
  private final List<ColumnMeta> metadata;
  private final TdsRowMetadata rowMetadata; // Cache metadata for efficiency

  TdsRow(List<byte[]> columnData, List<ColumnMeta> metadata) {
    this.columnData = columnData;
    this.metadata = metadata;
    this.rowMetadata = new TdsRowMetadata(metadata);
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

    // Simple check for NVARCHAR (0xE7) - uses UTF-16LE per TDS spec
    int dataType = meta.getDataType() & 0xFF;
    if ( dataType == 0xE7) {
      return type.cast(new String(data, StandardCharsets.UTF_16LE));
    }

    if (dataType == 0x7F || dataType == 0x26) {
      return type.cast(
          switch (meta.getMaxLength()) {
            case 1:
              byte byt = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).get();
              yield Byte.valueOf(byt);
            case 2:
              short sht = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort();
              yield Short.valueOf(sht);
            case 4:
              int i = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
              yield Integer.valueOf(i);
            case 8:
              long l = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
              yield Long.valueOf(l);
            default:
              throw new UnsupportedOperationException("Conversion not yet implemented for type: 0x"
                  + Integer.toHexString(meta.getDataType() & 0xFF));

          }
      );
    }

    if (dataType == 0x28) {
      // Little-endian unsigned 24-bit integer
      long days = ((data[2] & 0xFFL) << 16) |
          ((data[1] & 0xFFL) <<  8) |
          (data[0] & 0xFFL);

      // SQL Server DATE epoch = 0001-01-01
      return type.cast(LocalDate.of(1, 1, 1).plusDays(days));

    }

    if (dataType == 0x2a) {
      if (data.length == 0) {
        return type.cast(null);
      }
      if (data.length != 7) {
        throw new IllegalArgumentException("DATETIME2(3) bytes must be 7 bytes long");
      }

      // 1. Extract Time (Bytes 0-3, Little-Endian)
      // Ticks are in milliseconds for precision (3)
      long millisSinceMidnight = (data[0] & 0xFF) |
          ((data[1] & 0xFF) << 8) |
          ((data[2] & 0xFF) << 16) |
          ((long)(data[3] & 0xFF) << 24);

      // 2. Extract Date (Bytes 4-6, Little-Endian)
      int daysSinceEpoch = (data[4] & 0xFF) |
          ((data[5] & 0xFF) << 8) |
          ((data[6] & 0xFF) << 16);

      // 3. Construct the result
      LocalDate date = LocalDate.of(1, 1, 1).plusDays(daysSinceEpoch);
      LocalTime time = LocalTime.ofNanoOfDay(millisSinceMidnight * 1_000_000); // ms to ns

      return type.cast(LocalDateTime.of(date, time));
    }

    throw new UnsupportedOperationException("Conversion not yet implemented for type: 0x"
      + Integer.toHexString(meta.getDataType() & 0xFF));
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