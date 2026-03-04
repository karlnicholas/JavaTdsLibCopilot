package org.tdslib.javatdslib.internal;

import io.r2dbc.spi.Result;

/**
 * Represents an update count segment in a result stream. This class implements the
 * {@link Result.UpdateCount} interface and holds the number of rows affected by an update
 * operation.
 */
public class TdsUpdateCount implements Result.UpdateCount {
  private final long value;

  /**
   * Constructs a new TdsUpdateCount.
   *
   * @param value The number of rows updated.
   */
  public TdsUpdateCount(long value) {
    this.value = value;
  }

  @Override
  public long value() {
    return value;
  }
}
