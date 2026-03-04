package org.tdslib.javatdslib.internal;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;

/**
 * Represents a row segment in a result stream. This class wraps a {@link Row} and implements the
 * {@link Result.RowSegment} interface, allowing it to be emitted as part of a reactive result
 * stream.
 */
public class TdsRowSegment implements Result.RowSegment {
  private final Row row;

  /**
   * Constructs a new TdsRowSegment.
   *
   * @param row The row to be wrapped in this segment.
   */
  public TdsRowSegment(Row row) {
    this.row = row;
  }

  @Override
  public Row row() {
    return row;
  }
}
