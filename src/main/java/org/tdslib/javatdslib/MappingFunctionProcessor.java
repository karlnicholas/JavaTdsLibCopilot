package org.tdslib.javatdslib;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.Flow;
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
      // 1. Attempt the mapping (this executes your lambda)
      // 2. If successful, pass it downstream
      downstream.onNext(mapper.apply(row, row.getMetadata()));
    } catch (Throwable t) {
      // 3. If the lambda throws, we MUST catch it and signal onError
      // This effectively "propagates" the exception to your Client
      downstream.onError(t);
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