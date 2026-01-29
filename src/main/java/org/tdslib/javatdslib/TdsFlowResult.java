package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.util.concurrent.Flow;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class TdsFlowResult implements Result {
  private final TdsMessage queryMessage;
  private final TdsTransport transport;

  public TdsFlowResult(TdsMessage queryMessage, TdsTransport transport) {
    this.queryMessage = queryMessage;
    this.transport = transport;
  }

  @Override
  public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    return subscriber -> {
      QueryResponseTokenVisitor source = new QueryResponseTokenVisitor(transport, queryMessage);
      // We need a middle-man (Processor) to apply the mapping function
      source.subscribe(new MappingSubscriber<>(subscriber, mappingFunction));
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
