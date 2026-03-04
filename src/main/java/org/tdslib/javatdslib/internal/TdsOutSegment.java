package org.tdslib.javatdslib.internal;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.Result;

/**
 * Represents an output segment in a result stream, containing output parameters returned from a
 * stored procedure execution. This class implements the {@link Result.OutSegment} interface.
 */
public class TdsOutSegment implements Result.OutSegment {
  private final OutParameters outParameters;

  /**
   * Constructs a new TdsOutSegment.
   *
   * @param outParameters The output parameters associated with this segment.
   */
  public TdsOutSegment(OutParameters outParameters) {
    this.outParameters = outParameters;
  }

  @Override
  public OutParameters outParameters() {
    return outParameters;
  }
}
