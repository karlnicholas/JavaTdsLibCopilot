package org.tdslib.javatdslib;

import org.tdslib.javatdslib.tokens.colmetadata.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.util.List;
import java.util.Optional;

public record RowMetadata(ColMetaDataToken colMetaData) {
  public Optional<ColumnMeta> getColumnMetadata(String name) {
    for( ColumnMeta column: colMetaData.getColumns()) {
        if( name.equalsIgnoreCase( column.getName())) {
            return Optional.of(column);
        }
    }
    return Optional.empty();
  }
  public Optional<ColumnMeta> getColumnMetadata(int index) {
    var columns = colMetaData.getColumns();

    if (index >= 0 && index < columns.size()) {
      // Use ofNullable in case the actual element is null
      return Optional.ofNullable(columns.get(index));
    } else {
      return Optional.empty();
    }
  }
  public List<? extends ColumnMeta> getColumnMetadatas() {
    return colMetaData.getColumns();
  };
}
