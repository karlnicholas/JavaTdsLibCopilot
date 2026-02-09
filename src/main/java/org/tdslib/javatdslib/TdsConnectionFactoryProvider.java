package org.tdslib.javatdslib;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

public class TdsConnectionFactoryProvider implements ConnectionFactoryProvider {

  // Define your unique driver name
  public static final String TDS_DRIVER = "javatdslib";

  /**
   * This returns the specific driver identifier this provider supports.
   * When the user sets .option(DRIVER, "tds"), this is what matches.
   */
  @Override
  public String getDriver() {
    return TDS_DRIVER;
  }

  /**
   * R2DBC calls this to create the actual factory instance.
   * You extract the raw configuration (host, port, user) here.
   */
  @Override
  public ConnectionFactory create(ConnectionFactoryOptions options) {
    // Validation (ensure required options exist)
    // ...

    // Return your main entry point
    return new TdsConnectionFactoryImpl(options);
  }

  /**
   * This logic determines if your driver claims the user's request.
   * The default implementation checks if options.getValue(DRIVER) equals getDriver().
   * You can override it for complex logic, but usually, the default is sufficient.
   */
  @Override
  public boolean supports(ConnectionFactoryOptions options) {
    return TDS_DRIVER.equals(options.getValue(DRIVER));
  }
}