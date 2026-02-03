package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
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
import java.util.List;

/**
 TDS implementation of R2DBC Statement for FLOW queries.
 */
public class TdsStatementImpl implements Statement {
  private final String query;
  private final TdsTransport transport;
  private final List<List<ParamEntry>> batchParams = new ArrayList<>();
  // The current row's parameters
  private List<ParamEntry> currentParams = new ArrayList<>();

  public TdsStatementImpl(String sql, TdsTransport transport) {
    this.query = sql;
    this.transport = transport;
  }

  @Override
  public Publisher<Result> execute() {
    // R2DBC Rule: If bindings exist but add() wasn't called, treat it as the last row
    if (!currentParams.isEmpty()) {
      batchParams.add(new ArrayList<>(currentParams));
      currentParams.clear();
    }
    TdsMessage queryMsg;
    if ( currentParams.isEmpty()) {

      // Build SQL_BATCH payload: UTF-16LE string + NULL terminator (no length prefix)
      byte[] sqlBytes = (query).getBytes(StandardCharsets.UTF_16LE);

      byte[] allHeaders = AllHeaders.forAutoCommit(1).toBytes();

      ByteBuffer payload = ByteBuffer.allocate(allHeaders.length + sqlBytes.length);
      payload.put(allHeaders);
      payload.put(sqlBytes);

      payload.flip();

      // Create SQL_BATCH message
      queryMsg = TdsMessage.createRequest(PacketType.SQL_BATCH.getValue(), payload);

    } else {
      // Loop through 'batchParams' and send multiple RPC calls
      // OR send one RPC call with bundled parameters if the server supports it.
      // For a simple start, you can verify if you need to emit multiple Results
      // or combine them.

      // Note: To truly support Flux<Result>, you might need to chain execute calls
      // or construct a packet that handles multiple execution sets.
      RpcPacketBuilder rpcPacketBuilder = new RpcPacketBuilder(query, currentParams, true);
      ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPacket();
      // Create SQL_BATCH message
      queryMsg = TdsMessage.createRequest(PacketType.RPC_REQUEST.getValue(), rpcPacket);

    }
    // LAZY creation and execution of statement
    return subscriber -> {
      subscriber.onSubscribe(new Subscription() {
        private boolean completed;
        @Override
        public void request(long n) {
          // Use a boolean guard to ensure we run only once
          if (!completed && n > 0) {
            completed = true;
            subscriber.onNext(new TdsResultImpl(new QueryResponseTokenVisitor(transport, queryMsg)));
            subscriber.onComplete();
          }
        }
        @Override
        public void cancel() {}
      });
    };
  }

  // ───────────────────────────────────────────────────────────────
  // Named binds
  // ───────────────────────────────────────────────────────────────

  @Override
  public Statement bind(String param, Object value) {
    if (param == null || param.trim().isEmpty()) {
      throw new IllegalArgumentException("Parameter name cannot be null or empty");
    }

    if (value == null) {
      throw new IllegalArgumentException(
          "Value is null. Use bindNull(\"" + param + "\", <type>) instead of bind(\"" + param + "\", null)"
      );
    }

    // Infer BindingType from runtime value type
    BindingType bindingType = inferBindingType(value);

    if (bindingType == null) {
      throw new IllegalArgumentException(
          "Cannot bind value of type " + value.getClass().getName() +
              " — no matching BindingType found. Supported: Byte, Short, Integer, Long, Boolean, " +
              "Float, Double, BigDecimal, String, byte[], etc."
      );
    }

    BindingKey key = new BindingKey(bindingType, param);
    currentParams.add(new ParamEntry(key, value));

    return this;
  }

  @Override
  public Statement bindNull(String param, Class<?> type) {
    if (param == null || param.trim().isEmpty()) {
      throw new IllegalArgumentException("Parameter name cannot be null or empty");
    }
    if (type == null) {
      throw new IllegalArgumentException("Type for null parameter cannot be null");
    }

    // Infer BindingType from the provided Class<?>
    BindingType bindingType = inferBindingTypeForClass(type);

    if (bindingType == null) {
      throw new IllegalArgumentException(
          "Cannot bind null for type " + type.getName() +
              " — no matching BindingType found. Provide a supported type (e.g. Integer.class, String.class, etc.)"
      );
    }

    BindingKey key = new BindingKey(bindingType, param);
    currentParams.add(new ParamEntry(key, null));  // null value signals NULL

    return this;
  }

  /**
   * Infer BindingType from runtime value (non-null case).
   */
  private BindingType inferBindingType(Object value) {
    Class<?> clazz = value.getClass();

    return inferBindingTypeForClass(clazz);
  }

  /**
   * Infer BindingType from Class<?> for bindNull (null value case).
   */
  private BindingType inferBindingTypeForClass(Class<?> clazz) {
    if (clazz == Byte.class || clazz == Byte.TYPE) return BindingType.TINYINT;
    if (clazz == Short.class || clazz == Short.TYPE) return BindingType.SMALLINT;
    if (clazz == Integer.class || clazz == Integer.TYPE) return BindingType.INT;
    if (clazz == Long.class || clazz == Long.TYPE) return BindingType.BIGINT;

    if (clazz == Boolean.class || clazz == Boolean.TYPE) return BindingType.BIT;

    if (clazz == Float.class || clazz == Float.TYPE) return BindingType.REAL;
    if (clazz == Double.class || clazz == Double.TYPE) return BindingType.FLOAT;

    if (clazz == java.math.BigDecimal.class) return BindingType.DECIMAL;

    if (clazz == String.class) return BindingType.NVARCHAR;
    if (clazz == byte[].class) return BindingType.VARBINARY;

    // Add more as needed
    return null;  // unsupported
  }

  @Override
  public Statement fetchSize(int rows) {
//    this.fetchSize = rows;
    return this;
  }

  @Override
  public Statement add() {
    // Save the current row's parameters into the batch list
    if (!currentParams.isEmpty()) {
      batchParams.add(new ArrayList<>(currentParams));
      currentParams.clear();
    }
    return this;
  }

  @Override
  public Statement bind(int i, Object o) {
    return null;
  }

  @Override
  public Statement bindNull(int i, Class<?> aClass) {
    return null;
  }

  @Override
  public Statement returnGeneratedValues(String... columns) {
    return null;
  }

}