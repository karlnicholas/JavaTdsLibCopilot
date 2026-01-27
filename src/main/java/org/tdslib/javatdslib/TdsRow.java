package org.tdslib.javatdslib;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.nio.charset.StandardCharsets;
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
    if ((meta.getDataType() & 0xFF) == 0xE7) {
      return type.cast(new String(data, StandardCharsets.UTF_16LE));
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