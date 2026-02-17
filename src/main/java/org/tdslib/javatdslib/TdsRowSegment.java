package org.tdslib.javatdslib;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;

// 1. Row Segment
class TdsRowSegment implements Result.RowSegment {
  private final Row row;

  TdsRowSegment(Row row) {
    this.row = row;
  }

  @Override
  public Row row() {
    return row;
  }
}
