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

public class TdsStatement implements Statement {

  private final String query;
  private final TdsTransport transport;
  private final List<List<ParamEntry>> batchParams = new ArrayList<>();
  private List<ParamEntry> currentParams = new ArrayList<>();
  private int fetchSize = 0;

  public TdsStatement(TdsTransport transport, String query) {
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

    Parameter p = (value instanceof Parameter) ? (Parameter) value : Parameters.in(value);
    TdsType tdsType = resolveTdsType(p);

    if (tdsType == null) {
      throw new IllegalArgumentException("Unsupported parameter type: " + p.getType());
    }

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
    TdsType tdsType = TdsType.inferFromJavaType(type);
    if (tdsType == null) throw new IllegalArgumentException("Unsupported type for NULL: " + type.getName());

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

    // Return the Publisher<Result> routed through our BatchResultSplitter!
    return new Publisher<Result>() {
      @Override
      public void subscribe(Subscriber<? super Result> subscriber) {
        try {
          TdsMessage message;
          if (isSimpleBatch) {
            message = createSqlBatchMessage(query);
          } else {
            // Send the ENTIRE batch payload as one message!
            message = createRpcMessage(query, executions);
          }

          // 1. Send the single request to the wire, get a flat stream of segments back
          QueryResponseTokenVisitor flatSegmentStream = new QueryResponseTokenVisitor(transport, message);

          // 2. Wrap it in the Batch Splitter to chunk it into distinct R2DBC Results
          // (based on DONE/DONEPROC tokens).
          BatchResultSplitter resultPublisher = new BatchResultSplitter(flatSegmentStream);

          // 3. Subscribe the user's listener
          resultPublisher.subscribe(subscriber);

        } catch (Exception e) {
          // Immediately fail the stream if message creation or subscription fails
          subscriber.onSubscribe(new Subscription() {
            @Override public void request(long n) {}
            @Override public void cancel() {}
          });
          subscriber.onError(e);
        }
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

  private TdsMessage createRpcMessage(String sql, List<List<ParamEntry>> executions) {
    RpcPacketBuilder builder = new RpcPacketBuilder(sql, executions, true, transport.getVarcharCharset());
    ByteBuffer payload = builder.buildRpcPacket();
    return TdsMessage.createRequest(PacketType.RPC_REQUEST.getValue(), payload);
  }

  @Override
  public Statement fetchSize(int rows) {
    this.fetchSize = rows;
    return this;
  }

  private TdsType resolveTdsType(Parameter p) {
    Type t = p.getType();
    if (t instanceof R2dbcType rType) return TdsType.forR2dbcType(rType);
    if (t instanceof Type.InferredType iType) return TdsType.inferFromJavaType(iType.getJavaType());
    if (p.getValue() != null) return TdsType.inferFromJavaType(p.getValue().getClass());
    return null;
  }
}