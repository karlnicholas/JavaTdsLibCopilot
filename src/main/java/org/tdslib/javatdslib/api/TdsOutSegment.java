package org.tdslib.javatdslib.api;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.Result;

// 3. Out Parameters Segment (Unchanged)
public class TdsOutSegment implements Result.OutSegment {
  private final OutParameters outParameters;

  public TdsOutSegment(OutParameters outParameters) {
    this.outParameters = outParameters;
  }

  @Override
  public OutParameters outParameters() {
    return outParameters;
  }
}
