package org.tdslib.javatdslib;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

class TdsRowMetadataImpl implements RowMetadata {
  private final List<ColumnMeta> metadata;

  TdsRowMetadataImpl(List<ColumnMeta> metadata) {
    this.metadata = metadata;
  }

  @Override
  public ColumnMetadata getColumnMetadata(int index) {
    if (index < 0 || index >= metadata.size()) {
      throw new IndexOutOfBoundsException("Column index " + index + " is out of range");
    }
    return new TdsColumnMetadataImpl(metadata.get(index));
  }

  @Override
  public ColumnMetadata getColumnMetadata(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Column name cannot be null");
    }
    return metadata.stream()
        .filter(m -> m.getName().equalsIgnoreCase(name))
        .findFirst()
        .map(TdsColumnMetadataImpl::new)
        .orElseThrow(() -> new NoSuchElementException("Column name '" + name + "' does not exist"));
  }

  /**
   * Updated to return List<? extends ColumnMetadata> to match R2DBC SPI.
   */
  @Override
  public List<? extends ColumnMetadata> getColumnMetadatas() {
    return metadata.stream()
        .map(TdsColumnMetadataImpl::new)
        .collect(Collectors.toList());
  }
}