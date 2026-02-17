package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
  import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class TdsResult implements Result {
  // Source is now generic Segments, not just Rows
  private final Publisher<Result.Segment> source;

  public TdsResult(Publisher<Result.Segment> source) {
    this.source = source;
  }

  @Override
  public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    return subscriber -> source.subscribe(new Subscriber<>() {
      Subscription s;

      @Override
      public void onSubscribe(Subscription s) {
        this.s = s;
        subscriber.onSubscribe(s);
      }

      @Override
      public void onNext(Result.Segment segment) {
        // Filter: We only care about RowSegments here
        if (segment instanceof Result.RowSegment) {
          Row row = ((Result.RowSegment) segment).row();
          try {
            T mapped = mappingFunction.apply(row, row.getMetadata());
            subscriber.onNext(mapped);
          } catch (Exception e) {
            subscriber.onError(e);
            s.cancel();
          }
        } else {
          // Skip other segments (Messages, Counts)
          s.request(1);
        }
      }

      @Override
      public void onError(Throwable t) { subscriber.onError(t); }

      @Override
      public void onComplete() { subscriber.onComplete(); }
    });
  }

  @Override
  public Publisher<Long> getRowsUpdated() {
    return subscriber -> source.subscribe(new Subscriber<>() {
      Subscription s;

      @Override
      public void onSubscribe(Subscription s) {
        this.s = s;
        subscriber.onSubscribe(s);
      }

      @Override
      public void onNext(Result.Segment segment) {
        // Filter: We only care about UpdateCount segments
        if (segment instanceof Result.UpdateCount) {
          subscriber.onNext(((Result.UpdateCount) segment).value());
        } else {
          // Skip Rows, Messages, etc.
          s.request(1);
        }
      }

      @Override
      public void onError(Throwable t) { subscriber.onError(t); }

      @Override
      public void onComplete() { subscriber.onComplete(); }
    });
  }

  @Override
  public Result filter(Predicate<Result.Segment> predicate) {
    // Return a new Result wrapping a filtered Publisher
    // (Simplified implementation; assumes external filtering logic if needed)
    return new TdsResult(subscriber -> source.subscribe(new Subscriber<>() {
      Subscription s;
      @Override
      public void onSubscribe(Subscription s) {
        this.s = s;
        subscriber.onSubscribe(s);
      }
      @Override
      public void onNext(Result.Segment segment) {
        if (predicate.test(segment)) {
          subscriber.onNext(segment);
        } else {
          s.request(1);
        }
      }
      @Override
      public void onError(Throwable t) { subscriber.onError(t); }
      @Override
      public void onComplete() { subscriber.onComplete(); }
    }));
  }

  @Override
  public <T> Publisher<T> flatMap(Function<Result.Segment, ? extends Publisher<? extends T>> function) {
    // This allows advanced users to process the entire stream (Rows + Counts + OutParams)
    // Implementing purely with Publisher/Subscriber is complex (requires merge/concat logic).
    // If you are not using Project Reactor, you might leave this as unsupported or
    // implement a simple sequential processor.
    throw new UnsupportedOperationException("flatMap not fully implemented yet without Project Reactor");
  }
}