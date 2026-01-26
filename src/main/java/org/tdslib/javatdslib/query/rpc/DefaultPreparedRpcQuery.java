package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.RowWithMetadata;
import org.tdslib.javatdslib.TdsClient;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

public class DefaultPreparedRpcQuery implements PreparedRpcQuery {

  private final String sql;
  private final List<ParamEntry> params = new ArrayList<>();
  private int fetchSize = 0; // default unlimited
  private Duration timeout = null;

  public DefaultPreparedRpcQuery(String sql) {
    this.sql = sql;
  }

  // ───────────────────────────────────────────────────────────────
  // Named binds
  // ───────────────────────────────────────────────────────────────

  @Override
  public PreparedRpcQuery bind(String param, Object value) {
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
  public PreparedRpcQuery bindNull(String param, Class<?> type) {
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
  public PreparedRpcQuery fetchSize(int rows) {
    this.fetchSize = rows;
    return this;
  }

  @Override
  public PreparedRpcQuery timeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  @Override
  public Flow.Publisher<RowWithMetadata> execute(TdsClient client) {
    // TODO: Implement TDS RPC execution using params list
    RpcPacketBuilder rpcPacketBuilder = new RpcPacketBuilder(sql, params, true);
    ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPacket();
//    RpcPacketBuildersave rpcPacketBuilder = new RpcPacketBuildersave();
//    ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPayload("Michael", "Thomas", "mt@mt.com", 12);
    return client.rpcAsync(rpcPacket);
  }
  @Override
  public Flow.Publisher<RowWithMetadata> executeQuery(TdsClient client) {
    // TODO: Implement TDS RPC execution using params list
    RpcPacketBuilder rpcPacketBuilder = new RpcPacketBuilder(sql, params, true);
    ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPacket();
//    RpcPacketBuildersave rpcPacketBuilder = new RpcPacketBuildersave();
//    ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPayload("Michael", "Thomas", "mt@mt.com", 12);
    return client.rpcAsync(rpcPacket);
  }
  @Override
  public Flow.Publisher<RowWithMetadata> executeUpdate(TdsClient client) {
    // TODO: Implement TDS RPC execution using params list
    RpcPacketBuilder rpcPacketBuilder = new RpcPacketBuilder(sql, params, true);
    ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPacket();
//    RpcPacketBuildersave rpcPacketBuilder = new RpcPacketBuildersave();
//    ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPayload("Michael", "Thomas", "mt@mt.com", 12);
    return client.rpcAsync(rpcPacket);
  }
}