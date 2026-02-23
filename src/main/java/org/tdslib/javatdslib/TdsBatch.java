package org.tdslib.javatdslib;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TdsBatch implements Batch {

  private final TdsTransport transport;
  private final List<String> statements = new ArrayList<>();

  public TdsBatch(TdsTransport transport) {
    this.transport = transport;
  }

  @Override
  public Batch add(String sql) {
    if (sql == null) {
      throw new IllegalArgumentException("SQL cannot be null");
    }
    statements.add(sql);
    return this;
  }

  @Override
  public Publisher<? extends Result> execute() {
    return new Publisher<Result>() {
      @Override
      public void subscribe(Subscriber<? super Result> subscriber) {
        subscriber.onSubscribe(new Subscription() {
          private final AtomicBoolean completed = new AtomicBoolean(false);

          @Override
          public void request(long n) {
            if (n <= 0) {
              subscriber.onError(new IllegalArgumentException("n must be > 0"));
              return;
            }

            if (!completed.compareAndSet(false, true)) {
              return;
            }

            try {
              if (statements.isEmpty()) {
                subscriber.onComplete();
                return;
              }

              // Concatenate all statements. TDS SQL_BATCH handles multiple statements separated by spaces/newlines.
              String batchSql = String.join(";\n", statements);
              TdsMessage message = createSqlBatchMessage(batchSql);

              subscriber.onNext(new TdsResult(new QueryResponseTokenVisitor(transport, message)));
              subscriber.onComplete();
            } catch (Exception e) {
              subscriber.onError(e);
            }
          }

          @Override
          public void cancel() {
            completed.set(true);
          }
        });
      }
    };
  }

  // Reuse the exact same logic you wrote for TdsStatement
  private TdsMessage createSqlBatchMessage(String sql) {
    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
    byte[] headers = AllHeaders.forAutoCommit(1).toBytes();
    ByteBuffer payload = ByteBuffer.allocate(headers.length + sqlBytes.length);
    payload.put(headers);
    payload.put(sqlBytes);
    payload.flip();
    return TdsMessage.createRequest(PacketType.SQL_BATCH.getValue(), payload);
  }
}