package org.tdslib.javatdslib.api;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.codec.DecoderRegistry;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// 1. Row Segment (Unchanged)
public class TdsRowSegment implements Result.RowSegment {
  private final Row row;
  public TdsRowSegment(Row row) { this.row = row; }
  @Override public Row row() { return row; }
}

