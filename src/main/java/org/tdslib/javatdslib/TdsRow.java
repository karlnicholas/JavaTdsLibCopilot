package org.tdslib.javatdslib;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;
import org.tdslib.javatdslib.transport.CollationUtils;

import java.nio.charset.Charset;
import java.util.List;

class TdsRow implements Row {
  private final List<byte[]> columnData;
  private final List<ColumnMetadata> columnMetadata;
  private final TdsRowMetadata rowMetadata;
  private final Charset varcharCharset;

  TdsRow(List<byte[]> columnData, List<ColumnMetadata> columnMetadata, Charset varcharCharset) {
    this.columnData = columnData;
    this.columnMetadata = columnMetadata;
    this.rowMetadata = new TdsRowMetadata(columnMetadata);
    this.varcharCharset = varcharCharset;
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

    var typeInfo = ((ColumnMeta) meta.getNativeTypeMetadata()).getTypeInfo();
    TdsType tdsType = typeInfo.getTdsType();
    int scale = meta.getScale() != null ? meta.getScale() : 0;

    Charset resolvedCharset = varcharCharset;
    byte[] collationBytes = typeInfo.getCollation();

    if (collationBytes != null) {
      resolvedCharset = CollationUtils.getCharsetFromCollation(collationBytes)
          .orElse(varcharCharset);
    }

    // Use the new Registry
    return org.tdslib.javatdslib.decode.DecoderRegistry.DEFAULT.decode(data, tdsType, type, scale, resolvedCharset);
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    for (int i = 0; i < columnMetadata.size(); i++) {
      if (columnMetadata.get(i).getName().equalsIgnoreCase(name)) return get(i, type);
    }
    throw new IllegalArgumentException("Column not found: " + name);
  }
}