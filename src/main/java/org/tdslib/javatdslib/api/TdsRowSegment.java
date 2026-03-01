package org.tdslib.javatdslib.api;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;

// 1. Row Segment (Unchanged)
public class TdsRowSegment implements Result.RowSegment {
  private final Row row;
  public TdsRowSegment(Row row) { this.row = row; }
  @Override public Row row() { return row; }
}

