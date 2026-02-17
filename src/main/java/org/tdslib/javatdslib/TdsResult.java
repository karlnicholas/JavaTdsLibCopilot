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

  private final Publisher<Result.Segment> source;

  public TdsResult(Publisher<Result.Segment> source) {
    this.source = source;
  }

  @Override
  public Publisher<Long> getRowsUpdated() {
    return subscriber -> source.subscribe(new Subscriber<Result.Segment>() {
      Subscription subscription;

      @Override
      public void onSubscribe(Subscription s) {
        this.subscription = s;
        subscriber.onSubscribe(s);
      }

      @Override
      public void onNext(Result.Segment segment) {
        if (segment instanceof Result.UpdateCount) {
          subscriber.onNext(((Result.UpdateCount) segment).value());
        } else {
          // Not an update count (e.g., a Row or Message).
          // Skip it and request the next segment from the Visitor.
          subscription.request(1);
        }
      }

      @Override
      public void onError(Throwable t) {
        subscriber.onError(t);
      }

      @Override
      public void onComplete() {
        subscriber.onComplete();
      }
    });
  }

  @Override
  public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    if (mappingFunction == null) {
      throw new IllegalArgumentException("mappingFunction must not be null");
    }

    return subscriber -> source.subscribe(new Subscriber<Result.Segment>() {
      Subscription subscription;

      @Override
      public void onSubscribe(Subscription s) {
        this.subscription = s;
        subscriber.onSubscribe(s);
      }

      @Override
      public void onNext(Result.Segment segment) {
        if (segment instanceof Result.RowSegment) {
          Row row = ((Result.RowSegment) segment).row();
          try {
            T mapped = mappingFunction.apply(row, row.getMetadata());
            if (mapped == null) {
              subscriber.onError(new IllegalStateException("Mapping function returned null"));
              return;
            }
            subscriber.onNext(mapped);
          } catch (Exception e) {
            subscriber.onError(e);
            subscription.cancel();
          }
        } else {
          // Not a row (e.g., an UpdateCount).
          // Skip it and request the next segment from the Visitor.
          subscription.request(1);
        }
      }

      @Override
      public void onError(Throwable t) {
        subscriber.onError(t);
      }

      @Override
      public void onComplete() {
        subscriber.onComplete();
      }
    });
  }

  @Override
  public Result filter(Predicate<Result.Segment> predicate) {
    if (predicate == null) {
      throw new IllegalArgumentException("predicate must not be null");
    }

    // Return a new TdsResult backed by a filtered Publisher
    return new TdsResult(subscriber -> source.subscribe(new Subscriber<Result.Segment>() {
      Subscription subscription;

      @Override
      public void onSubscribe(Subscription s) {
        this.subscription = s;
        subscriber.onSubscribe(s);
      }

      @Override
      public void onNext(Result.Segment segment) {
        if (predicate.test(segment)) {
          subscriber.onNext(segment);
        } else {
          // Predicate failed. Skip it and request the next segment.
          subscription.request(1);
        }
      }

      @Override
      public void onError(Throwable t) {
        subscriber.onError(t);
      }

      @Override
      public void onComplete() {
        subscriber.onComplete();
      }
    }));
  }

  @Override
  public <T> Publisher<T> flatMap(Function<Result.Segment, ? extends Publisher<? extends T>> mappingFunction) {
    if (mappingFunction == null) {
      throw new IllegalArgumentException("mappingFunction must not be null");
    }
    // Implementing flatMap correctly without a library like Reactor/RxJava
    // (handling merge/concat of inner publishers) is extremely complex and error-prone.
    // Unless you have a specific requirement for this advanced operator right now,
    // it is safer to leave it unsupported.
    throw new UnsupportedOperationException("flatMap is not currently supported in TdsResult");
  }
}