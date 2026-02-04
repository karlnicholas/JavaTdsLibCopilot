package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.query.rpc.BindingKey;
import org.tdslib.javatdslib.query.rpc.BindingType;
import org.tdslib.javatdslib.query.rpc.ParamEntry;
import org.tdslib.javatdslib.query.rpc.RpcPacketBuilder;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TDS implementation of R2DBC Statement.
 * Supports batch execution via standard Reactive Streams Publisher logic (no Project Reactor).
 */
public class TdsStatementImpl implements Statement {

  private final String query;
  private final TdsTransport transport;

  // Holds sets of parameters for batch execution (e.g. multiple INSERT rows)
  private final List<List<ParamEntry>> batchParams = new ArrayList<>();
  // The current row's parameters being built
  private List<ParamEntry> currentParams = new ArrayList<>();

  public TdsStatementImpl(String sql, TdsTransport transport) {
    if (sql == null || sql.trim().isEmpty()) {
      throw new IllegalArgumentException("SQL cannot be null or empty");
    }
    this.query = sql;
    this.transport = transport;
  }

  @Override
  public Statement add() {
    // Snapshot current params into the batch list and prepare for next row
    if (!currentParams.isEmpty()) {
      batchParams.add(new ArrayList<>(currentParams));
      currentParams.clear();
    }
    return this;
  }

  @Override
  public Statement bind(String param, Object value) {
    if (value == null) throw new IllegalArgumentException("value cannot be null. Use bindNull.");
    BindingType type = inferBindingType(value.getClass());
    if (type == null) throw new IllegalArgumentException("Unsupported Java type: " + value.getClass().getName());
    currentParams.add(new ParamEntry(new BindingKey(type, param), value));
    return this;
  }

  @Override
  public Statement bind(int index, Object value) {
    return bind("@p" + index, value);
  }

  @Override
  public Statement bindNull(String param, Class<?> type) {
    BindingType bt = inferBindingType(type);
    if (bt == null) throw new IllegalArgumentException("Unsupported type for NULL: " + type.getName());
    currentParams.add(new ParamEntry(new BindingKey(bt, param), null));
    return this;
  }

  @Override
  public Statement bindNull(int index, Class<?> type) {
    return bindNull("@p" + index, type);
  }

  @Override
  public Publisher<Result> execute() {
    // 1. Finalize the last set of params if 'add()' wasn't called explicitly
    if (!currentParams.isEmpty()) {
      batchParams.add(new ArrayList<>(currentParams));
      currentParams.clear();
    }

    // 2. Prepare the execution plan
    // If no params at all, it's a simple SQL Batch (1 execution)
    // If params exist, it's RPC (N executions)
    final List<List<ParamEntry>> executions;
    final boolean isSimpleBatch;

    if (batchParams.isEmpty()) {
      isSimpleBatch = true;
      executions = Collections.emptyList(); // Not used for simple batch
    } else {
      isSimpleBatch = false;
      executions = new ArrayList<>(batchParams); // Snapshot for safety
    }

    // 3. Return a Publisher that iterates the executions
    return new Publisher<Result>() {
      @Override
      public void subscribe(Subscriber<? super Result> subscriber) {
        subscriber.onSubscribe(new Subscription() {
          private final AtomicInteger index = new AtomicInteger(0);
          private final AtomicBoolean completed = new AtomicBoolean(false);

          @Override
          public void request(long n) {
            if (completed.get() || n <= 0) return;

            long processed = 0;
            while (processed < n && !completed.get()) {
              TdsMessage messageToSend = null;

              if (isSimpleBatch) {
                // Single SQL Batch Execution
                if (index.compareAndSet(0, 1)) {
                  messageToSend = createSqlBatchMessage(query);
                } else {
                  // Already done
                  completed.set(true);
                  subscriber.onComplete();
                  return;
                }
              } else {
                // Parameterized RPC Execution
                int i = index.getAndIncrement();
                if (i < executions.size()) {
                  messageToSend = createRpcMessage(query, executions.get(i));
                } else {
                  completed.set(true);
                  subscriber.onComplete();
                  return;
                }
              }

              if (messageToSend != null) {
                // Send Request & Wrap Response
                // Note: The TokenVisitor processes the response stream for THIS specific message
                Result result = new TdsResultImpl(new QueryResponseTokenVisitor(transport, messageToSend));
                subscriber.onNext(result);
                processed++;

                // If simple batch, we are done after 1 iteration
                if (isSimpleBatch) {
                  completed.set(true);
                  subscriber.onComplete();
                  return;
                }
              }
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

  // --- Helper Methods ---

  private TdsMessage createSqlBatchMessage(String sql) {
    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
    byte[] headers = AllHeaders.forAutoCommit(1).toBytes();
    ByteBuffer payload = ByteBuffer.allocate(headers.length + sqlBytes.length);
    payload.put(headers);
    payload.put(sqlBytes);
    payload.flip();
    return TdsMessage.createRequest(PacketType.SQL_BATCH.getValue(), payload);
  }

  private TdsMessage createRpcMessage(String sql, List<ParamEntry> params) {
    RpcPacketBuilder builder = new RpcPacketBuilder(sql, params, true);
    ByteBuffer payload = builder.buildRpcPacket();
    return TdsMessage.createRequest(PacketType.RPC_REQUEST.getValue(), payload);
  }

  private BindingType inferBindingType(Class<?> clazz) {
    if (clazz == Integer.class || clazz == int.class) return BindingType.INT;
    if (clazz == Long.class || clazz == long.class) return BindingType.BIGINT;
    if (clazz == String.class) return BindingType.NVARCHAR;
    if (clazz == Boolean.class || clazz == boolean.class) return BindingType.BIT;
    if (clazz == Double.class || clazz == double.class) return BindingType.FLOAT;
    if (clazz == java.time.LocalDate.class) return BindingType.DATE;
    if (clazz == java.time.LocalDateTime.class) return BindingType.DATETIME2;
    if (clazz == java.util.UUID.class) return BindingType.UNIQUEIDENTIFIER;
    if (clazz == java.math.BigDecimal.class) return BindingType.DECIMAL;
    // Add others as needed
    return null;
  }

  @Override
  public Statement fetchSize(int rows) { return this; }

  @Override
  public Statement returnGeneratedValues(String... columns) { return this; }
}