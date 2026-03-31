package org.tdslib.javatdslib.impl;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;

/**
 * Represents a non-fatal server message (TDS InfoToken) according to the R2DBC 1.0.0 SPI.
 */
public class TdsMessageSegment implements Result.Message {

  private final int errorCode;
  private final String sqlState;
  private final String message;
  private final R2dbcException exception;

  public TdsMessageSegment(int errorCode, String sqlState, String message) {
    this.errorCode = errorCode;
    this.sqlState = sqlState;
    this.message = message;

    // R2DBC 1.0.0 requires the message to also be available as a full R2dbcException.
    // Because R2dbcException is abstract, we create an anonymous subclass to hold the warning data.
    this.exception = new R2dbcException(message, sqlState, errorCode) {};
  }

  @Override
  public R2dbcException exception() {
    return exception;
  }

  @Override
  public int errorCode() {
    return errorCode;
  }

  @Override
  public String sqlState() {
    return sqlState;
  }

  @Override
  public String message() {
    return message;
  }
}