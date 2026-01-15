package org.tdslib.javatdslib;

import org.tdslib.javatdslib.tokens.colmetadata.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.util.List;

public record RowMetadata(ColMetaDataToken colMetaData) {
  ColumnMeta getColumnMetadata(String name) {
    return colMetaData.getColumns().get(0);
  };
  ColumnMeta getColumnMetadata(int index) {
    return colMetaData.getColumns().get(0);
  }
  List<? extends ColumnMeta> getColumnMetadatas() {
    return colMetaData.getColumns();
  };
}
