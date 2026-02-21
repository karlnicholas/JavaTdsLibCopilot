package org.tdslib.javatdslib;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.util.List;

class TdsRow implements Row {
  private final List<byte[]> columnData;
  private final List<ColumnMetadata> columnMetadata;
  private final TdsRowMetadata rowMetadata;

  TdsRow(List<byte[]> columnData, List<ColumnMetadata> columnMetadata) {
    this.columnData = columnData;
    this.columnMetadata = columnMetadata;
    this.rowMetadata = new TdsRowMetadata(columnMetadata);
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
    ColumnMetadata meta = columnMetadata.get(index);

    if (data == null) return null;

    TdsType tdsType = ((ColumnMeta) meta.getNativeTypeMetadata()).getTypeInfo().getTdsType();
    int scale = meta.getScale() != null ? meta.getScale() : 0;

    return TdsDataConverter.convert(data, tdsType, type, scale);
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    for (int i = 0; i < columnMetadata.size(); i++) {
      if (columnMetadata.get(i).getName().equalsIgnoreCase(name)) return get(i, type);
    }
    throw new IllegalArgumentException("Column not found: " + name);
  }
}