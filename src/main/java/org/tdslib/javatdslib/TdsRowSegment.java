package org.tdslib.javatdslib;

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.OutParametersMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;

// 1. Row Segment
class TdsRowSegment implements Result.RowSegment {
  private final Row row;

  TdsRowSegment(Row row) {
    this.row = row;
  }

  @Override
  public Row row() {
    return row;
  }
}

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

// 4. Out Parameters Implementation (Delegates to TdsRowImpl for type conversion)
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
    // TdsRowMetadataImpl implements RowMetadata, which is compatible
    // with OutParametersMetadata structure in most drivers,
    // or you can implement a wrapper if strictly required.
    // For now, assuming TdsRowMetadataImpl suffices or simple casting.
    return (OutParametersMetadata) delegate.getMetadata();
  }
}