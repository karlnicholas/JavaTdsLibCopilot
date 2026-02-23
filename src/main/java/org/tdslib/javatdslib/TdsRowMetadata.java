package org.tdslib.javatdslib;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

class TdsRowMetadata implements RowMetadata {
  private final List<ColumnMetadata> metadata;

  TdsRowMetadata(List<ColumnMetadata> metadata) {
    this.metadata = metadata;
  }

  @Override
  public ColumnMetadata getColumnMetadata(int index) {
    if (index < 0 || index >= metadata.size()) {
      throw new IndexOutOfBoundsException("Column index " + index + " is out of range");
    }
    return metadata.get(index);
  }

  @Override
  public ColumnMetadata getColumnMetadata(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Column name cannot be null");
    }
    return metadata.stream()
        .filter(m -> m.getName().equalsIgnoreCase(name))
        .findFirst()
        .orElseThrow(() -> new NoSuchElementException("Column name '" + name + "' does not exist"));
  }

  /**
   * Updated to return List<? extends ColumnMetadata> to match R2DBC SPI.
   */
  @Override
  public List<? extends ColumnMetadata> getColumnMetadatas() {
    return Collections.unmodifiableList(metadata);
  }
}