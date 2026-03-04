module org.tdslib.javatdslib {
  // 1. External dependencies mapped from your pom.xml
  requires r2dbc.spi;
  requires org.reactivestreams;
  requires org.slf4j;

  // 2. Export ONLY your clean public API
  exports org.tdslib.javatdslib.api;
  exports org.tdslib.javatdslib.reactive;
  exports org.tdslib.javatdslib.internal;

  // 3. Register the R2DBC Service Provider (replaces the META-INF/services file)
  provides io.r2dbc.spi.ConnectionFactoryProvider
      with org.tdslib.javatdslib.api.TdsConnectionFactoryProvider;
}