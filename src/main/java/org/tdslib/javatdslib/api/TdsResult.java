package org.tdslib.javatdslib.api;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Implementation of {@link Result} for the TDS protocol. This class represents the result of a
 * query execution, providing access to rows, update counts, and other result segments.
 */
public class TdsResult implements Result {
  private static final Logger logger = LoggerFactory.getLogger(TdsResult.class);
  private final Publisher<Result.Segment> source;

  /**
   * Primary constructor used by the networking layer.
   * Bridges the mechanical queue drainer into a standard Reactive Streams Publisher.
   * * (Note: To be updated in the future to connect directly to AsyncWorkerSink)
   */
  /**
   * Primary constructor.
   * Accepts a standard Reactive Publisher stream of Result.Segments.
   */
  public TdsResult(Publisher<Result.Segment> source) {
    this.source = source;
  }

  @Override
  public Publisher<Long> getRowsUpdated() {
    // R2DBC Spec: getRowsUpdated only emits UpdateCount segments
    return Flux.from(source)
        .ofType(Result.UpdateCount.class)
        .map(Result.UpdateCount::value);
  }

  @Override
  public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    if (mappingFunction == null) throw new IllegalArgumentException("mappingFunction must not be null");

    // R2DBC Spec: map only applies to RowSegments. UpdateCounts are ignored.
    return Flux.from(source)
        .ofType(Result.RowSegment.class)
        .map(segment -> mappingFunction.apply(segment.row(), segment.row().getMetadata()));
  }

  @Override
  public Result filter(Predicate<Result.Segment> predicate) {
    if (predicate == null) throw new IllegalArgumentException("predicate must not be null");

    // R2DBC Spec: filter applies to the entire segment stream, returning a new Result
    return new TdsResult(Flux.from(source).filter(predicate));
  }

  @Override
  public <T> Publisher<T> flatMap(Function<Result.Segment, ? extends Publisher<? extends T>> mappingFunction) {
    if (mappingFunction == null) throw new IllegalArgumentException("mappingFunction must not be null");

    // R2DBC Spec: flatMap applies to all segments (Rows, OutParameters, UpdateCounts)
    return Flux.from(source).flatMap(mappingFunction);
  }
}