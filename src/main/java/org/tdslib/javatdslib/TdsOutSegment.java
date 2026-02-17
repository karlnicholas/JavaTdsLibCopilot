package org.tdslib.javatdslib;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.Result;

// 3. Out Parameters Segment
class TdsOutSegment implements Result.OutSegment {
  private final OutParameters outParameters;

  TdsOutSegment(OutParameters outParameters) {
    this.outParameters = outParameters;
  }

  @Override
  public OutParameters outParameters() {
    return outParameters;
  }
}
