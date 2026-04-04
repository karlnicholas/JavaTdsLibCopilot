package org.tdslib.javatdslib.impl;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.Type;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.codec.EncoderRegistry;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.protocol.TdsServerErrorException;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;
import org.tdslib.javatdslib.reactive.R2dbcErrorTranslator;
import org.tdslib.javatdslib.reactive.R2dbcTypeMapper;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.RpcPacketBuilder;
import org.tdslib.javatdslib.transport.TdsTransport;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
  public Publisher<? extends Result> execute() {
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

    // Pass the recipe, determining which builder to call at subscription time
    return transport.execute(() -> {
          if (isSimpleBatch) {
            return createSqlBatchMessage(query);
          } else {
            return createRpcMessage(query, executions);
          }
        })
        .windowUntil(this::isBoundarySegment)
        .map(TdsResult::new)
        .onErrorMap(TdsServerErrorException.class, R2dbcErrorTranslator::translateException);
  }

  /**
   * Helper method for Project Reactor's windowUntil operator.
   * Determines if a segment marks the end of a specific SQL statement execution.
   */
  private boolean isBoundarySegment(Result.Segment segment) {
    return segment instanceof TdsUpdateCount
        || segment instanceof Result.OutSegment;
  }

  private TdsMessage createSqlBatchMessage(String sql) {
    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
    ByteBuffer payload = ByteBuffer.wrap(sqlBytes);

    AllHeaders headers = context.isInTransaction()
        ? AllHeaders.forActiveTransaction(context.getTransactionDescriptor(), 1)
        : AllHeaders.forAutoCommit(1);

    return TdsMessage.createWithHeaders(PacketType.SQL_BATCH, headers, payload);
  }

  private TdsMessage createRpcMessage(String sql, List<List<TdsParameter>> executions) {
    EncoderRegistry registry = EncoderRegistry.DEFAULT;
    RpcEncodingContext encodingContext =
        new RpcEncodingContext(context.getVarcharCharset(), context.getCurrentCollationBytes());

    RpcPacketBuilder builder =
        new RpcPacketBuilder(sql, executions, true, registry, encodingContext);
    ByteBuffer payload = builder.buildRpcPacket();

    AllHeaders headers = context.isInTransaction()
        ? AllHeaders.forActiveTransaction(context.getTransactionDescriptor(), 1)
        : AllHeaders.forAutoCommit(1);

    return TdsMessage.createWithHeaders(PacketType.RPC_REQUEST, headers, payload);
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