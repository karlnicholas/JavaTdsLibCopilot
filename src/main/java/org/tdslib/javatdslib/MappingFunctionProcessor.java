package org.tdslib.javatdslib;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.BiFunction;

// A generic version of your IncrementProcessor logic
public class MappingFunctionProcessor<T, R> implements Processor<T, R> {
  private final BiFunction<Row, RowMetadata, ? extends R> mapper;
  private Subscriber<? super R> downstream;

  public MappingFunctionProcessor(BiFunction<Row, RowMetadata, ? extends R> mapper) {
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
    Row rowCast = (Row) row;
    RowMetadata rowMetadata = rowCast.getMetadata();
    downstream.onNext(mapper.apply(rowCast, rowMetadata));
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