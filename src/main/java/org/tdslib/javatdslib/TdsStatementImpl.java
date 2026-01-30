package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.query.rpc.BindingKey;
import org.tdslib.javatdslib.query.rpc.BindingType;
import org.tdslib.javatdslib.query.rpc.ParamEntry;
import org.tdslib.javatdslib.query.rpc.RpcPacketBuilder;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 TDS implementation of R2DBC Statement for FLOW queries.
 */
public class TdsStatementImpl implements Statement {
//  private final QueryResponseTokenVisitor source;
//  private final TdsClient client;
  private final String query;
  private final TdsTransport transport;
  private final List<ParamEntry> params = new ArrayList<>();
  private boolean completed;

  public TdsStatementImpl(String sql, TdsTransport transport) {
//    this.source = source;
//    this.completed = false;
    this.query = sql;
    this.transport = transport;
  }

//  @Override
//  public Publisher<Result> execute() {
//    // We return a Publisher that emits a single FlowResult
//    return subscriber -> {
//      subscriber.onSubscribe(new Subscription() {
//        @Override
//        public void request(long n) {
//          // Use a boolean guard to ensure we run only once
//          if (!completed && n > 0) {
//            completed = true;
//            subscriber.onNext(new TdsResultImpl(source));
//            subscriber.onComplete();
//          }
//        }
//        @Override
//        public void cancel() {}
//      });
//    };
//  }

  @Override
  public Publisher<Result> execute() {
    // TODO: Implement TDS RPC execution using params list
    RpcPacketBuilder rpcPacketBuilder = new RpcPacketBuilder(query, params, true);
    ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPacket();
  //    RpcPacketBuildersave rpcPacketBuilder = new RpcPacketBuildersave();
  //    ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPayload("Michael", "Thomas", "mt@mt.com", 12);
    // Create SQL_BATCH message
    TdsMessage queryMsg = TdsMessage.createRequest(PacketType.RPC_REQUEST.getValue(), rpcPacket);

    // 2. Instead of blocking send/receive:
    //    → queue the message, register OP_WRITE if needed
    //    → return future that will be completed from selector loop

//    return new TdsStatementImpl();

//    return new TdsResultImpl(new QueryResponseTokenVisitor(transport, queryMsg));
    // client.rpcAsync(rpcPacket);
    return subscriber -> {
      subscriber.onSubscribe(new Subscription() {
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

//    String cleanParam = param.trim();
//    if (cleanParam.startsWith("@")) {
//      cleanParam = cleanParam.substring(1);
//    }

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
    params.add(new ParamEntry(key, value));

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

    // Normalize param name
//    String cleanParam = param.trim();
//    if (cleanParam.startsWith("@")) {
//      cleanParam = cleanParam.substring(1);
//    }

    // Infer BindingType from the provided Class<?>
    BindingType bindingType = inferBindingTypeForClass(type);

    if (bindingType == null) {
      throw new IllegalArgumentException(
          "Cannot bind null for type " + type.getName() +
              " — no matching BindingType found. Provide a supported type (e.g. Integer.class, String.class, etc.)"
      );
    }

    BindingKey key = new BindingKey(bindingType, param);
    params.add(new ParamEntry(key, null));  // null value signals NULL

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

  //  private String normalizeParamName(String param) {
  //    if (param == null) return null;
  //    if (param.startsWith("@") || param.startsWith(":")) {
  //      return param.substring(1);
  //    }
  //    return param;
  //  }

  @Override
  public Statement fetchSize(int rows) {
//    this.fetchSize = rows;
    return this;
  }

  @Override
  public Statement add() {
    return null;
  }

  @Override
  public Statement bind(int i, Object o) {
    return null;
  }

//  @Override
//  public Statement bind(String s, Object o) {
//    return null;
//  }

  @Override
  public Statement bindNull(int i, Class<?> aClass) {
    return null;
  }

//  @Override
//  public Statement bindNull(String s, Class<?> aClass) {
//    return null;
//  }

  @Override
  public Statement returnGeneratedValues(String... columns) {
    return null;
  }

//  @Override
//  public Statement fetchSize(int rows) {
//    return Statement.super.fetchSize(rows);
//  }
}