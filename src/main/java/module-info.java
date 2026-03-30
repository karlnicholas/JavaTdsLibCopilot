import org.tdslib.javatdslib.impl.TdsConnectionFactoryProvider;

module org.tdslib.javatdslib {
  // 1. External dependencies mapped from your pom.xml
  requires transitive r2dbc.spi;
  requires org.reactivestreams;
  requires org.slf4j;
  requires reactor.core;

  // 2. Export ONLY your clean public API
  exports org.tdslib.javatdslib.api;
  exports org.tdslib.javatdslib.impl;

  // 3. Register the R2DBC Service Provider
  provides io.r2dbc.spi.ConnectionFactoryProvider
      with org.tdslib.javatdslib.impl.TdsConnectionFactoryProvider;
}