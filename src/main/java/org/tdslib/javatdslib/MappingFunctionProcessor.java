package org.tdslib.javatdslib;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.BiFunction;

// A generic version of your IncrementProcessor logic
public class MappingFunctionProcessor<T extends Row, R> implements Processor<T, R> {
  private final BiFunction<T, RowMetadata, ? extends R>  mapper;
  private Subscriber<? super R> downstream;

  public MappingFunctionProcessor(BiFunction<T, RowMetadata, ? extends R>  mapper) {
    this.mapper = mapper;
  }

  @Override
  public void subscribe(Subscriber<? super R> subscriber) {
    this.downstream = subscriber;
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    downstream.onSubscribe(subscription);
  }

  @Override
  public void onNext(T row) {
    try {
      R result = mapper.apply(row, row.getMetadata());
      if (result == null) {
        // Option A: Fail (Strict Spec)
        onError(new NullPointerException("Mapper returned null"));

        // Option B: Just return (Drop the value - rare but sometimes desired)
        // return;
      } else {
        downstream.onNext(result);
      }
    } catch (Throwable t) {
      onError(t);
    }
  }
  @Override
  public void onError(Throwable throwable) {
    downstream.onError(throwable);
  }

  @Override
  public void onComplete() {
    downstream.onComplete();
  }
}