package org.tdslib.javatdslib;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

class TdsRowMetadata implements RowMetadata {
  private final List<ColumnMeta> metadata;

  TdsRowMetadata(List<ColumnMeta> metadata) {
    this.metadata = metadata;
  }

  @Override
  public ColumnMetadata getColumnMetadata(int index) {
    if (index < 0 || index >= metadata.size()) {
      throw new IndexOutOfBoundsException("Column index " + index + " is out of range");
    }
    return new TdsColumnMetadata(metadata.get(index));
  }

  @Override
  public ColumnMetadata getColumnMetadata(String name) {
    return metadata.stream()
        .filter(m -> m.getName().equalsIgnoreCase(name))
        .findFirst()
        .map(TdsColumnMetadata::new)
        .orElseThrow(() -> new NoSuchElementException("Column name '" + name + "' does not exist"));
  }

  @Override
  public Collection<? extends ColumnMetadata> getColumnMetadatas() {
    return metadata.stream()
        .map(TdsColumnMetadata::new)
        .collect(Collectors.toList());
  }

  @Override
  public Collection<String> getColumnNames() {
    return metadata.stream()
        .map(ColumnMeta::getName)
        .collect(Collectors.toList());
  }
}