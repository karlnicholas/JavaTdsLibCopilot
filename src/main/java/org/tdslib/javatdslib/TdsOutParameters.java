package org.tdslib.javatdslib;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;

// 4. Out Parameters Implementation (Delegates to TdsRow for type conversion)
class TdsOutParameters implements OutParameters {
  private final TdsRow delegate;

  TdsOutParameters(TdsRow delegate) {
    this.delegate = delegate;
  }

  @Override
  public <T> T get(int index, Class<T> type) {
    return delegate.get(index, type);
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    return delegate.get(name, type);
  }

  @Override
  public OutParametersMetadata getMetadata() {
    // TdsRowMetadata implements RowMetadata, which is compatible
    // with OutParametersMetadata structure in most drivers,
    // or you can implement a wrapper if strictly required.
    // For now, assuming TdsRowMetadata suffices or simple casting.
    return (OutParametersMetadata) delegate.getMetadata();
  }
}