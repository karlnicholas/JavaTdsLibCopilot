package org.tdslib.javatdslib.api;

import io.r2dbc.spi.Option;

/**
 * Custom configuration options for the TDS (SQL Server) R2DBC Driver.
 * These options can be passed to the R2DBC ConnectionFactory builder.
 */
public final class TdsLibOptions {

  // 1. Private constructor prevents anyone (even you) from instantiating this class
  private TdsLibOptions() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  // 2. Your public API constants
  public static final Option<Boolean> TRUST_SERVER_CERTIFICATE =
      Option.valueOf("trustServerCertificate");

  public static final Option<String> TRUST_STORE =
      Option.valueOf("trustStore");

  public static final Option<String> TRUST_STORE_PASSWORD =
      Option.valueOf("trustStorePassword");
}