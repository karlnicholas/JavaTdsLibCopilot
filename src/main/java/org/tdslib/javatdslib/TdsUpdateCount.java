package org.tdslib.javatdslib;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Result;

// 2. Update Count Segment
class TdsUpdateCount implements Result.UpdateCount {
  private final long value;

  TdsUpdateCount(long value) {
    this.value = value;
  }

  @Override
  public long value() {
    return value;
  }
}
