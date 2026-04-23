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
import org.tdslib.javatdslib.packets.OutboundTdsMessage;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.protocol.TdsServerErrorException;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;
import org.tdslib.javatdslib.reactive.R2dbcErrorTranslator;
import org.tdslib.javatdslib.reactive.R2dbcTypeMapper;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.RpcPacketBuilder;
import org.tdslib.javatdslib.transport.TdsTransport;
import reactor.core.publisher.Mono;

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

  /**
   * Constructs a new TdsStatement.
   *
   * @param transport The TDS transport.
   * @param context   The connection context.
   * @param query     The SQL query string.
   */
  public TdsStatement(TdsTransport transport, ConnectionContext context, String query) {
    this.transport = transport;
    this.context = context;
    this.query = query;
  }

  /**
   * Adds the current set of bound parameters to the
   * batch and clears the current parameters for the next set.
   *
   * @return This statement.
   */
  @Override
  public Statement add() {
    if (!currentParams.isEmpty()) {
      batchParams.add(new ArrayList<>(currentParams));
      currentParams.clear();
    }
    return this;
  }

  /**
   * Binds a value to a parameter by its 0-indexed position.
   *
   * @param index The 0-based index.
   * @param value The value to bind.
   * @return This statement.
   */
  @Override
  public Statement bind(int index, Object value) {
    return bind("@p" + index, value);
  }

  /**
   * Binds a value to a named parameter.
   *
   * @param name  The name of the parameter.
   * @param value The value to bind.
   * @return This statement.
   */
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

    // Ensure the parameter name always starts with '@' ---
    String safeName = name.startsWith("@") ? name : "@" + name;

    currentParams.add(
        new TdsParameter(tdsType, safeName, p.getValue(), p instanceof Parameter.Out)
    );
    return this;
  }

  /**
   * Binds a null value to a parameter by its 0-indexed position.
   *
   * @param index The 0-based index.
   * @param type  The expected Java type of the parameter.
   * @return This statement.
   */
  @Override
  public Statement bindNull(int index, Class<?> type) {
    return bindNull("@p" + index, type);
  }

  /**
   * Binds a null value to a named parameter.
   *
   * @param name The name of the parameter.
   * @param type  The expected Java type of the parameter.
   * @return This statement.
   */
  @Override
  public Statement bindNull(String name, Class<?> type) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null");
    }
    TdsType tdsType = TdsType.inferFromJavaType(type);
    if (tdsType == null) {
      throw new IllegalArgumentException("Unsupported type for NULL: " + type.getName());
    }

    // --- FIX: Ensure the parameter name always starts with '@' ---
    String safeName = name.startsWith("@") ? name : "@" + name;

    currentParams.add(new TdsParameter(tdsType, safeName, null, false));
    return this;
  }

  /**
   * Configures the statement to return generated values. (Currently a no-op implementation).
   *
   * @param columns The names of the columns for which to return generated values.
   * @return This statement.
   */
  @Override
  public Statement returnGeneratedValues(String... columns) {
    return this;
  }

  /**
   * Executes the statement and returns a publisher for the results.
   *
   * @return A publisher of {@link Result} objects.
   */
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

    return transport.execute(headers -> {
      if (isSimpleBatch) {
        return createSqlBatchMessage(query, headers);
      } else {
        return createRpcMessage(query, executions, headers);
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

  /**
   * Creates a TDS SQL Batch message for simple, non-parameterized execution.
   *
   * @param sql The SQL query string.
   * @return A {@link OutboundTdsMessage} ready for transport.
   */
  private OutboundTdsMessage createSqlBatchMessage(String sql, AllHeaders headers) {
    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
    ByteBuffer payload = ByteBuffer.wrap(sqlBytes);

    // Wrap in Mono.just()
    return OutboundTdsMessage.createWithHeaders(PacketType.SQL_BATCH, headers, Mono.just(payload));
  }

//  private TdsMessage createSqlBatchMessage(String sql, AllHeaders headers) {
//    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
//    ByteBuffer payload = ByteBuffer.wrap(sqlBytes);
//    return TdsMessage.createWithHeaders(PacketType.SQL_BATCH, headers, payload);
//  }

  /**
   * Creates a TDS RPC Request message for parameterized or batched execution.
   *
   * @param sql        The SQL query string.
   * @param executions The list of parameter sets to execute.
   * @return A {@link OutboundTdsMessage} ready for transport.
   */
  private OutboundTdsMessage createRpcMessage(
      String sql, List<List<TdsParameter>> executions, AllHeaders headers) {
    EncoderRegistry registry = EncoderRegistry.DEFAULT;
    RpcEncodingContext encodingContext =
        new RpcEncodingContext(context.getVarcharCharset(), context.getCurrentCollationBytes());

    RpcPacketBuilder builder =
        new RpcPacketBuilder(sql, executions, registry, encodingContext);
    ByteBuffer payload = builder.buildRpcPacket();

    // Wrap in Mono.just()
    return OutboundTdsMessage.createWithHeaders(PacketType.RPC_REQUEST, headers, Mono.just(payload));
  }

//  private TdsMessage createRpcMessage(
//      String sql, List<List<TdsParameter>> executions, AllHeaders headers) {
//    EncoderRegistry registry = EncoderRegistry.DEFAULT;
//    RpcEncodingContext encodingContext =
//        new RpcEncodingContext(context.getVarcharCharset(), context.getCurrentCollationBytes());
//
//    RpcPacketBuilder builder =
//        new RpcPacketBuilder(sql, executions, registry, encodingContext);
//    ByteBuffer payload = builder.buildRpcPacket();
//    return TdsMessage.createWithHeaders(PacketType.RPC_REQUEST, headers, payload);
//  }

  /**
   * Configures the fetch size for the statement.
   *
   * @param rows The number of rows to fetch.
   * @return This statement.
   */
  @Override
  public Statement fetchSize(int rows) {
    this.fetchSize = rows;
    return this;
  }

  /**
   * Resolves the appropriate {@link TdsType} for a given R2DBC {@link Parameter}.
   *
   * @param p The parameter to resolve.
   * @return The corresponding TDS type, or {@code null} if it cannot be resolved.
   */
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
