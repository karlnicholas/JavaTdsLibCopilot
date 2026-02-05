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

public class TdsStatementImpl implements Statement {

  private final String query;
  private final TdsTransport transport;
  private final List<List<ParamEntry>> batchParams = new ArrayList<>();
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
    if (type == null) {
      throw new IllegalArgumentException("Unsupported Java type: " + value.getClass().getName());
    }
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
    if (!currentParams.isEmpty()) {
      batchParams.add(new ArrayList<>(currentParams));
      currentParams.clear();
    }

    final List<List<ParamEntry>> executions;
    final boolean isSimpleBatch;

    if (batchParams.isEmpty()) {
      isSimpleBatch = true;
      executions = Collections.emptyList();
    } else {
      isSimpleBatch = false;
      executions = new ArrayList<>(batchParams);
    }

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
                if (index.compareAndSet(0, 1)) {
                  messageToSend = createSqlBatchMessage(query);
                } else {
                  completed.set(true);
                  subscriber.onComplete();
                  return;
                }
              } else {
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
                Result result = new TdsResultImpl(new QueryResponseTokenVisitor(transport, messageToSend));
                subscriber.onNext(result);
                processed++;

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
    // --- Exact Numerics ---
    if (clazz == Integer.class || clazz == int.class) return BindingType.INT;
    if (clazz == Long.class || clazz == long.class) return BindingType.BIGINT;
    if (clazz == Short.class || clazz == short.class) return BindingType.SMALLINT;
    if (clazz == Byte.class || clazz == byte.class) return BindingType.TINYINT;
    if (java.math.BigDecimal.class.isAssignableFrom(clazz)) return BindingType.DECIMAL;

    // --- Approximate Numerics ---
    if (clazz == Double.class || clazz == double.class) return BindingType.FLOAT;
    if (clazz == Float.class || clazz == float.class) return BindingType.REAL;

    // --- Boolean ---
    if (clazz == Boolean.class || clazz == boolean.class) return BindingType.BIT;

    // --- Date and Time ---
    if (clazz == java.time.LocalDate.class) return BindingType.DATE;
    if (clazz == java.time.LocalTime.class) return BindingType.TIME;
    if (clazz == java.time.LocalDateTime.class) return BindingType.DATETIME2;
    if (clazz == java.time.OffsetDateTime.class) return BindingType.DATETIMEOFFSET;

    // --- Strings (Default to Unicode) ---
    if (clazz == String.class) return BindingType.NVARCHAR;

    // --- Binary ---
    if (java.nio.ByteBuffer.class.isAssignableFrom(clazz) || clazz == byte[].class) {
      return BindingType.VARBINARY;
    }

    // --- Other ---
    if (clazz == java.util.UUID.class) return BindingType.UNIQUEIDENTIFIER;

    return null;
  }

  @Override
  public Statement fetchSize(int rows) { return this; }
  @Override
  public Statement returnGeneratedValues(String... columns) { return this; }
}