package org.tdslib.javatdslib.protocol;

import io.r2dbc.spi.Result;

// 2. Update Count Segment (Unchanged)
public class TdsUpdateCount implements Result.UpdateCount {
  private final long value;

  public TdsUpdateCount(long value) {
    this.value = value;
  }

  @Override
  public long value() {
    return value;
  }
}
