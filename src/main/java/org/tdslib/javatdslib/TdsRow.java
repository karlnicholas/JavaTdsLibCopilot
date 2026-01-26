package org.tdslib.javatdslib;

import io.r2dbc.spi.Row;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.nio.charset.StandardCharsets;
import java.util.List;

class TdsRow implements Row {
  private final List<byte[]> columnData;
  private final List<ColumnMeta> metadata;

  TdsRow(List<byte[]> columnData, List<ColumnMeta> metadata) {
    this.columnData = columnData;
    this.metadata = metadata;
  }

  @Override
  public <T> T get(int index, Class<T> type) {
    if (index < 0 || index >= columnData.size()) {
      throw new IndexOutOfBoundsException("Index: " + index);
    }

    byte[] data = columnData.get(index);
    if (data == null) return null;

    ColumnMeta meta = metadata.get(index);
    byte tdsType = meta.getDataType();

    // Handle Unicode Strings (NVARCHAR/NCHAR - 0xE7/0xEF)
    if ((tdsType & 0xFF) == 0xE7 || (tdsType & 0xFF) == 0xEF) {
      String val = new String(data, StandardCharsets.UTF_16LE);
      return type.cast(val);
    }

    // Additional type conversions (Integer, Long, etc.) based on tdsType would go here
    throw new UnsupportedOperationException("Type conversion not implemented for: 0x"
        + Integer.toHexString(tdsType & 0xFF));
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