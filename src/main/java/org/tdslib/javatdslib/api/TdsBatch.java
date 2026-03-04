package org.tdslib.javatdslib.api;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.api.reactive.ReactiveResultVisitor;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * An implementation of the R2DBC {@link Batch} interface for executing multiple SQL statements as a
 * single unit. This class buffers the SQL statements and sends them to the server in a single
 * request when executed.
 */
public class TdsBatch implements Batch {

  private final TdsTransport transport;
  private final ConnectionContext context;
  private final List<String> statements = new ArrayList<>();

  /**
   * Constructs a new TdsBatch.
   *
   * @param transport The transport layer for sending the batch execution request.
   * @param context The connection context associated with this batch.
   */
  public TdsBatch(TdsTransport transport, ConnectionContext context) {
    this.transport = transport;
    this.context = context;
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
        subscriber.onSubscribe(
            new Subscription() {
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

                  String batchSql = String.join(";\n", statements);
                  TdsMessage message = createSqlBatchMessage(batchSql);

                  // FIX: Removed TokenDispatcher, just pass transport, context, and message
                  subscriber.onNext(
                      new TdsResult(
                          new ReactiveResultVisitor(transport, context, message)));
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