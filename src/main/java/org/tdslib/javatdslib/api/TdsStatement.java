package org.tdslib.javatdslib.api;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.codec.EncoderRegistry;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.protocol.TdsServerErrorException;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;
import org.tdslib.javatdslib.reactive.DataSink;
import org.tdslib.javatdslib.reactive.R2dbcErrorTranslator;
import org.tdslib.javatdslib.reactive.R2dbcTypeMapper;
import org.tdslib.javatdslib.reactive.TdsResultBatchSequencer;
import org.tdslib.javatdslib.reactive.TdsResultStreamHandler;
import org.tdslib.javatdslib.tokens.visitors.CompositeTokenVisitor;
import org.tdslib.javatdslib.tokens.visitors.EnvChangeVisitor;
import org.tdslib.javatdslib.tokens.visitors.MessageVisitor;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.RpcPacketBuilder;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * Implementation of {@link Statement} for the TDS protocol. This class allows for the execution of
 * SQL queries and stored procedures, supporting parameter binding and batch execution.
 */
public class TdsStatement implements Statement {

  private final String query;
  private final TdsTransport transport;
  private final ConnectionContext context;
  private final List<List<TdsParameter>> batchParams = new ArrayList<>();
  private List<TdsParameter> currentParams = new ArrayList<>();
  private int fetchSize = 0;

  /**
   * Constructs a new TdsStatement.
   *
   * @param transport The transport layer for sending the statement execution request.
   * @param context The connection context associated with this statement.
   * @param query The SQL query or stored procedure call to be executed.
   */
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

    currentParams.add(new TdsParameter(tdsType, name, p.getValue(), p instanceof Parameter.Out));
    return this;
  }

  @Override
  public Statement bindNull(int index, Class<?> type) {
    return bindNull("@p" + index, type);
  }

  @Override
  public Statement bindNull(String name, Class<?> type) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null");
    }
    TdsType tdsType = TdsType.inferFromJavaType(type);
    if (tdsType == null) {
      throw new IllegalArgumentException("Unsupported type for NULL: " + type.getName());
    }

    currentParams.add(new TdsParameter(tdsType, name, null, false));
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

    final List<List<TdsParameter>> executions;
    final boolean isSimpleBatch;

    if (batchParams.isEmpty()) {
      isSimpleBatch = true;
      executions = Collections.emptyList();
    } else {
      isSimpleBatch = false;
      executions = new ArrayList<>(batchParams);
    }

    return subscriber -> {
      try {
        TdsMessage message;
        if (isSimpleBatch) {
          message = createSqlBatchMessage(query);
        } else {
          message = createRpcMessage(query, executions);
        }

        TdsResultStreamHandler segmentVisitor =
            new TdsResultStreamHandler(transport, context, message);

        CompositeTokenVisitor pipeline =
            new CompositeTokenVisitor(
                new EnvChangeVisitor(context),
                new MessageVisitor(segmentVisitor::onError),
                segmentVisitor);

        segmentVisitor.setVisitorChain(pipeline);

        // --- ADAPTER 1: Bridge the pure-Java stream handler to the reactive batch sequencer
        Publisher<Result.Segment> segmentPublisher = s -> {
          segmentVisitor.setSink(new DataSink<Result.Segment>() {
            @Override public void pushNext(Result.Segment item) { s.onNext(item); }
            @Override public void pushComplete() { s.onComplete(); }
            @Override public void pushError(Throwable t) { s.onError(t); }
          });
          s.onSubscribe(new Subscription() {
            @Override public void request(long n) { segmentVisitor.increaseDemand(n); }
            @Override public void cancel() { segmentVisitor.cancel(); }
          });
        };

        TdsResultBatchSequencer resultSequencer = new TdsResultBatchSequencer(segmentPublisher);

        // --- ADAPTER 2: Bridge the pure-Java batch sequencer to the user's R2DBC Subscriber
        resultSequencer.setSink(
            new DataSink<Result>() {
              @Override
              public void pushNext(Result result) {
                subscriber.onNext(result);
              }

              @Override
              public void pushComplete() {
                subscriber.onComplete();
              }

              @Override
              public void pushError(Throwable t) {
                // Translate internal TDS errors to R2DBC SPI errors at the boundary
                if (t instanceof TdsServerErrorException tdsError) {
                  subscriber.onError(R2dbcErrorTranslator.translateException(tdsError));
                } else {
                  subscriber.onError(t); // Pass through non-database errors
                }
              }
            });

        subscriber.onSubscribe(
            new Subscription() {
              @Override
              public void request(long n) {
                resultSequencer.increaseDemand(n);
              }

              @Override
              public void cancel() {
                resultSequencer.cancel();
              }
            });

      } catch (Exception e) {
        subscriber.onSubscribe(
            new Subscription() {
              @Override
              public void request(long n) {}

              @Override
              public void cancel() {}
            });
        subscriber.onError(e);
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

  private TdsMessage createRpcMessage(String sql, List<List<TdsParameter>> executions) {
    EncoderRegistry registry = EncoderRegistry.DEFAULT;
    RpcEncodingContext encodingContext =
        new RpcEncodingContext(context.getVarcharCharset(), context.getCurrentCollationBytes());

    RpcPacketBuilder builder =
        new RpcPacketBuilder(sql, executions, true, registry, encodingContext);
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
    if (t instanceof R2dbcType rdbcType) {
      return R2dbcTypeMapper.toTdsType(rdbcType);
    }
    if (t instanceof Type.InferredType inferredType) {
      return TdsType.inferFromJavaType(inferredType.getJavaType());
    }
    if (p.getValue() != null) {
      return TdsType.inferFromJavaType(p.getValue().getClass());
    }
    return null;
  }
}