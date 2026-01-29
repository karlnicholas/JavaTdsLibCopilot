package org.tdslib.javatdslib;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MappingSubscriber<T> implements Subscriber<Row> {
  private final Subscriber<? super T> downstream;
  private final BiFunction<Row, RowMetadata, ? extends T> mapper;

  public MappingSubscriber(Subscriber<? super T> downstream, BiFunction<Row, RowMetadata, ? extends T> mapper) {
    this.downstream = downstream;
    this.mapper = mapper;
  }

  @Override public void onSubscribe(Subscription s) { downstream.onSubscribe(s); }
  @Override public void onNext(Row row) { downstream.onNext(mapper.apply(row, row.getMetadata())); }
  @Override public void onError(Throwable t) { downstream.onError(t); }
  @Override public void onComplete() { downstream.onComplete(); }
}