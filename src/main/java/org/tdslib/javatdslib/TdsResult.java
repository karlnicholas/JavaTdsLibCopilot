package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.util.List;
import java.util.function.BiFunction;

public class TdsResult implements Result {
  private final List<ColumnMeta> metadata;
  // Use a Sinks.Many or a custom Emitter to bridge the async tokens to the stream
  private final Sinks.Many<List<byte[]>> rowSink = Sinks.many().unicast().onBackpressureBuffer();

  public TdsResult(List<ColumnMeta> metadata) {
    this.metadata = metadata;
  }

  // Internal method called by the Visitor
  void pushRow(List<byte[]> rowData) {
    rowSink.tryEmitNext(rowData);
  }

  void complete() {
    rowSink.tryEmitComplete();
  }

  @Override
  public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    TdsRowMetadata rowMetadata = new TdsRowMetadata(this.metadata);

    return rowSink.asFlux().map(columnData -> {
      // TdsRow is a view over the column data list
      TdsRow row = new TdsRow(columnData, metadata);
      return mappingFunction.apply(row, rowMetadata);
    });
  }
}