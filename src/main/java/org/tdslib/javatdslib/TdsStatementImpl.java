package org.tdslib.javatdslib;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.query.rpc.BindingKey;
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

// REMOVED: static import ...BindingType.inferBindingType;

public class TdsStatementImpl implements Statement {

  private final String query;
  private final TdsTransport transport;
  private final List<List<ParamEntry>> batchParams = new ArrayList<>();
  private List<ParamEntry> currentParams = new ArrayList<>();
  private int fetchSize = 0; // 0 means default/unlimited

  public TdsStatementImpl(TdsTransport transport, String query) {
    this.transport = transport;
    this.query = query;
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
  public Statement bind(int index, Object value) {
    return bind("@p" + index, value);
  }

  @Override
  public Statement bind(String name, Object value) {
    if (value == null) {
      throw new IllegalArgumentException("value cannot be null. Use bindNull.");
    }

    // 1. Canonicalize to R2DBC Parameter
    // (If user passed raw Int, wrap it. If they passed Parameters.in(), use it)
    Parameter p = (value instanceof Parameter)
            ? (Parameter) value
            : Parameters.in(value);

    // 2. Resolve TdsType
    ((R2dbcType)p.getType()).
    TdsType tdsType = resolveTdsType(p);

    if (tdsType == null) {
      throw new IllegalArgumentException("Unsupported parameter type: " + p.getType());
    }

    // 3. Add to parameters
    // Note: Ensure your BindingKey class is updated to accept TdsType!
    currentParams.add(new ParamEntry(new BindingKey(tdsType, name), p));
    return this;
  }

  @Override
  public Statement bindNull(int index, Class<?> type) {
    return bindNull("@p" + index, type);
  }

  @Override
  public Statement bindNull(String name, Class<?> type) {
    if (type == null) throw new IllegalArgumentException("Type cannot be null");

    // Resolve directly from Class
    TdsType tdsType = TdsType.inferFromJavaType(type);
    if (tdsType == null) {
      throw new IllegalArgumentException("Unsupported type for NULL: " + type.getName());
    }

    currentParams.add(new ParamEntry(new BindingKey(tdsType, name), null));
    return this;
  }

  @Override
  public Statement returnGeneratedValues(String... columns) {
    return this;
  }

  @Override
  public Publisher<Result> execute() {
    if (batchParams.isEmpty() && !currentParams.isEmpty()) {
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
            if (completed.get() || n <= 0) {
              subscriber.onError(new IllegalStateException("Statement already completed"));
              return;
            }

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
              } else {
                subscriber.onError(new IllegalStateException("Cannot determine request for SQL SERVER"));
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

  @Override
  public Statement fetchSize(int rows) {
    this.fetchSize = rows;
    return this;
  }

//  // --- Helper to resolve TdsType from R2DBC Parameter ---
//  private TdsType resolveTdsType(Parameter p) {
//    Type t = p.getType();
//
//    // A. Explicit R2DBC Type
//    if (t instanceof R2dbcType rType) {
//      return TdsType.forR2dbcType(rType);
//    }
//
//    // B. Inferred Type (Class)
//    if (t instanceof Type.InferredType iType) {
//      return TdsType.inferFromJavaType(iType.getJavaType());
//    }
//
//    // C. Value Fallback
//    if (p.getValue() != null) {
//      return TdsType.inferFromJavaType(p.getValue().getClass());
//    }
//    return null;
//  }
}