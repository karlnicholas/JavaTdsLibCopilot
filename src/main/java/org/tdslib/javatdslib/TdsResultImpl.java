package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class TdsResultImpl implements Result {
  private final Publisher<Row> source;

  public TdsResultImpl(Publisher<Row> source) {
    this.source = source;
  }

  @Override
  public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    // Return a new Publisher. When someone subscribes to THIS...
    return subscriber -> {
      // create a mapping processor, give it the mapping function and the subscriber
      MappingFunctionProcessor<Row, T> processor = new MappingFunctionProcessor<>(mappingFunction);
      // 1. First, tell the processor who the final subscriber is
      processor.subscribe(subscriber);
      // 2. ONLY THEN, connect the processor to the data source
      source.subscribe(processor);
    };
  }

  @Override
  public Publisher<Long> getRowsUpdated() {
    return null;
  }

  @Override
  public Result filter(Predicate<Segment> predicate) {
    return null;
  }

  @Override
  public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> function) {
    return null;
  }
}