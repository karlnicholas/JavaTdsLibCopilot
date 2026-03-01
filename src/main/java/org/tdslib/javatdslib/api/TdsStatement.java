package org.tdslib.javatdslib.api;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.codec.EncoderRegistry;
import org.tdslib.javatdslib.reactive.BatchResultSplitter;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.protocol.rpc.BindingKey;
import org.tdslib.javatdslib.protocol.rpc.ParamEntry;
import org.tdslib.javatdslib.protocol.rpc.RpcPacketBuilder;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.tokens.TokenParserRegistry;
import org.tdslib.javatdslib.tokens.visitors.CompositeTokenVisitor;
import org.tdslib.javatdslib.tokens.visitors.EnvChangeVisitor;
import org.tdslib.javatdslib.tokens.visitors.MessageVisitor;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;
import org.tdslib.javatdslib.tokens.visitors.ReactiveResultVisitor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TdsStatement implements Statement {

  private final String query;
  private final TdsTransport transport;
  private final ConnectionContext context;
  private final List<List<ParamEntry>> batchParams = new ArrayList<>();
  private List<ParamEntry> currentParams = new ArrayList<>();
  private int fetchSize = 0;

  public TdsStatement(TdsTransport transport, ConnectionContext context, String query) {
    this.transport = transport;
    this.context = context;
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

    return new Publisher<Result>() {
      @Override
      public void subscribe(Subscriber<? super Result> subscriber) {
        try {
          TdsMessage message;
          if (isSimpleBatch) {
            message = createSqlBatchMessage(query);
          } else {
            message = createRpcMessage(query, executions);
          }

          TokenDispatcher dispatcher = new TokenDispatcher(TokenParserRegistry.DEFAULT);

          // 1. Instantiate the Segment Producer
          ReactiveResultVisitor segmentVisitor = new ReactiveResultVisitor(transport, context, message, dispatcher);

          // 2. Compose the Pipeline
          CompositeTokenVisitor pipeline = new CompositeTokenVisitor(
              new EnvChangeVisitor(context),
              new MessageVisitor(segmentVisitor::emitStreamError),
              segmentVisitor
          );

          // 3. Attach pipeline to the producer
          segmentVisitor.setVisitorChain(pipeline);

          BatchResultSplitter resultPublisher = new BatchResultSplitter(segmentVisitor);
          resultPublisher.subscribe(subscriber);

        } catch (Exception e) {
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
    // 1. Use the shared stateless registry
    EncoderRegistry registry = EncoderRegistry.DEFAULT;

    // 2. Wrap the connection context properties
    RpcEncodingContext encodingContext = new RpcEncodingContext(
        context.getVarcharCharset(),
        context.getCurrentCollationBytes()
    );

    // 3. Pass dependencies to the builder
    RpcPacketBuilder builder = new RpcPacketBuilder(
        sql,
        batchParams,
        true,
        registry,
        encodingContext
    );
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