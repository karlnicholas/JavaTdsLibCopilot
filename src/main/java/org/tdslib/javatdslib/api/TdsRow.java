package org.tdslib.javatdslib.api;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.nio.charset.Charset;
import java.util.List;
import org.tdslib.javatdslib.codec.DecoderRegistry;
import org.tdslib.javatdslib.protocol.CollationUtils;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;

/**
 * Implementation of {@link Row} for the TDS protocol. This class represents a single row of data in
 * a result set, providing access to column values by index or name.
 */
public class TdsRow implements Row {
  private final List<Object> columnData;
  private final List<ColumnMetadata> columnMetadata;
  private final TdsRowMetadata rowMetadata;
  private final Charset varcharCharset;

  /**
   * Constructs a new TdsRow.
   *
   * @param columnData The raw byte data for each column in the row.
   * @param columnMetadata The metadata for each column in the row.
   * @param varcharCharset The charset to use for decoding VARCHAR columns.
   */
  public TdsRow(
      List<Object> columnData, List<ColumnMetadata> columnMetadata, Charset varcharCharset) {
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

    // FIX: Retrieve as Object first
    Object rawData = columnData.get(index);
    ColumnMetadata meta = columnMetadata.get(index);

    if (rawData == null) {
      return null;
    }

    // --- MVC INTERCEPT: Return the Clob proxy directly ---
    if (rawData instanceof org.tdslib.javatdslib.streaming.TdsClob && type.isAssignableFrom(io.r2dbc.spi.Clob.class)) {
      return type.cast(rawData);
    }

    // If it's not a streaming proxy, it must be a standard byte[] payload
    byte[] data = (byte[]) rawData;

    var typeInfo = ((ColumnMeta) meta.getNativeTypeMetadata()).getTypeInfo();
    TdsType tdsType = typeInfo.getTdsType();
    int scale = meta.getScale() != null ? meta.getScale() : 0;

    Charset resolvedCharset = varcharCharset;
    byte[] collationBytes = typeInfo.getCollation();

    if (collationBytes != null) {
      resolvedCharset =
          CollationUtils.getCharsetFromCollation(collationBytes).orElse(varcharCharset);
    }

    // Use the new Registry
    return DecoderRegistry.DEFAULT.decode(data, tdsType, type, scale, resolvedCharset);
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    for (int i = 0; i < columnMetadata.size(); i++) {
      if (columnMetadata.get(i).getName().equalsIgnoreCase(name)) {
        return get(i, type);
      }
    }
    throw new IllegalArgumentException("Column not found: " + name);
  }
}
