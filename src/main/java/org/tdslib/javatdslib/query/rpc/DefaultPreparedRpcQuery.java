package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.RowWithMetadata;
import org.tdslib.javatdslib.TdsClient;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
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
  // Implied positional binds (no index provided → auto-increment)
  // ───────────────────────────────────────────────────────────────

  @Override
  public PreparedRpcQuery bindShort(Short value) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.SHORT, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindInteger(Integer value) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.INTEGER, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindLong(Long value) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.LONG, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindString(String value) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.STRING, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBytes(byte[] value) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.BYTES, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBoolean(Boolean value) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.BOOLEAN, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindSQLXML(SQLXML xml) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.SQLXML, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, xml));
    return this;
  }

  @Override
  public PreparedRpcQuery bindNClob(NClob nclob) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.NCLOB, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, nclob));
    return this;
  }

  @Override
  public PreparedRpcQuery bindClob(Clob clob) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.CLOB, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, clob));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBlob(Blob blob) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.BLOB, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, blob));
    return this;
  }

  @Override
  public PreparedRpcQuery bindTimestamp(Timestamp ts) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.TIMESTAMP, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, ts));
    return this;
  }

  @Override
  public PreparedRpcQuery bindTime(Time time) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.TIME, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, time));
    return this;
  }

  @Override
  public PreparedRpcQuery bindDate(Date date) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.DATE, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, date));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBigDecimal(BigDecimal bd) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.BIGDECIMAL, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, bd));
    return this;
  }

  @Override
  public PreparedRpcQuery bindDouble(Double d) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.DOUBLE, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, d));
    return this;
  }

  @Override
  public PreparedRpcQuery bindFloat(Float f) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.FLOAT, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, f));
    return this;
  }

  @Override
  public PreparedRpcQuery bindByte(Byte b) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingType.BYTE, BindingKind.IMPLIED, null, order);
    params.add(new ParamEntry(key, b));
    return this;
  }

  // ───────────────────────────────────────────────────────────────
  // Indexed binds (explicit position)
  // ───────────────────────────────────────────────────────────────

  @Override
  public PreparedRpcQuery bindShort(int index, Short value) {
    BindingKey key = new BindingKey(BindingType.SHORT, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindInteger(int index, Integer value) {
    BindingKey key = new BindingKey(BindingType.INTEGER, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindLong(int index, Long value) {
    BindingKey key = new BindingKey(BindingType.LONG, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindString(int index, String value) {
    BindingKey key = new BindingKey(BindingType.STRING, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBytes(int index, byte[] value) {
    BindingKey key = new BindingKey(BindingType.BYTES, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBoolean(int index, Boolean value) {
    BindingKey key = new BindingKey(BindingType.BOOLEAN, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindSQLXML(int index, SQLXML xml) {
    BindingKey key = new BindingKey(BindingType.SQLXML, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, xml));
    return this;
  }

  @Override
  public PreparedRpcQuery bindNClob(int index, NClob nclob) {
    BindingKey key = new BindingKey(BindingType.NCLOB, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, nclob));
    return this;
  }

  @Override
  public PreparedRpcQuery bindClob(int index, Clob clob) {
    BindingKey key = new BindingKey(BindingType.CLOB, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, clob));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBlob(int index, Blob blob) {
    BindingKey key = new BindingKey(BindingType.BLOB, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, blob));
    return this;
  }

  @Override
  public PreparedRpcQuery bindTimestamp(int index, Timestamp ts) {
    BindingKey key = new BindingKey(BindingType.TIMESTAMP, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, ts));
    return this;
  }

  @Override
  public PreparedRpcQuery bindTime(int index, Time time) {
    BindingKey key = new BindingKey(BindingType.TIME, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, time));
    return this;
  }

  @Override
  public PreparedRpcQuery bindDate(int index, Date date) {
    BindingKey key = new BindingKey(BindingType.DATE, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, date));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBigDecimal(int index, BigDecimal bd) {
    BindingKey key = new BindingKey(BindingType.BIGDECIMAL, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, bd));
    return this;
  }

  @Override
  public PreparedRpcQuery bindDouble(int index, Double d) {
    BindingKey key = new BindingKey(BindingType.DOUBLE, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, d));
    return this;
  }

  @Override
  public PreparedRpcQuery bindFloat(int index, Float f) {
    BindingKey key = new BindingKey(BindingType.FLOAT, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, f));
    return this;
  }

  @Override
  public PreparedRpcQuery bindByte(int index, Byte b) {
    BindingKey key = new BindingKey(BindingType.BYTE, BindingKind.INDEXED, null, index);
    params.add(new ParamEntry(key, b));
    return this;
  }

  // ───────────────────────────────────────────────────────────────
  // Named binds
  // ───────────────────────────────────────────────────────────────

  @Override
  public PreparedRpcQuery bindShort(String param, Short value) {
    BindingKey key = new BindingKey(BindingType.SHORT, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindInteger(String param, Integer value) {
    BindingKey key = new BindingKey(BindingType.INTEGER, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindLong(String param, Long value) {
    BindingKey key = new BindingKey(BindingType.LONG, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindString(String param, String value) {
    BindingKey key = new BindingKey(BindingType.STRING, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBytes(String param, byte[] value) {
    BindingKey key = new BindingKey(BindingType.BYTES, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBoolean(String param, Boolean value) {
    BindingKey key = new BindingKey(BindingType.BOOLEAN, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bindSQLXML(String param, SQLXML xml) {
    BindingKey key = new BindingKey(BindingType.SQLXML, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, xml));
    return this;
  }

  @Override
  public PreparedRpcQuery bindNClob(String param, NClob nclob) {
    BindingKey key = new BindingKey(BindingType.NCLOB, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, nclob));
    return this;
  }

  @Override
  public PreparedRpcQuery bindClob(String param, Clob clob) {
    BindingKey key = new BindingKey(BindingType.CLOB, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, clob));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBlob(String param, Blob blob) {
    BindingKey key = new BindingKey(BindingType.BLOB, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, blob));
    return this;
  }

  @Override
  public PreparedRpcQuery bindTimestamp(String param, Timestamp ts) {
    BindingKey key = new BindingKey(BindingType.TIMESTAMP, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, ts));
    return this;
  }

  @Override
  public PreparedRpcQuery bindTime(String param, Time time) {
    BindingKey key = new BindingKey(BindingType.TIME, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, time));
    return this;
  }

  @Override
  public PreparedRpcQuery bindDate(String param, Date date) {
    BindingKey key = new BindingKey(BindingType.DATE, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, date));
    return this;
  }

  @Override
  public PreparedRpcQuery bindBigDecimal(String param, BigDecimal bd) {
    BindingKey key = new BindingKey(BindingType.BIGDECIMAL, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, bd));
    return this;
  }

  @Override
  public PreparedRpcQuery bindDouble(String param, Double d) {
    BindingKey key = new BindingKey(BindingType.DOUBLE, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, d));
    return this;
  }

  @Override
  public PreparedRpcQuery bindFloat(String param, Float f) {
    BindingKey key = new BindingKey(BindingType.FLOAT, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, f));
    return this;
  }

  @Override
  public PreparedRpcQuery bindByte(String param, Byte b) {
    BindingKey key = new BindingKey(BindingType.BYTE, BindingKind.NAMED, param, -1);
    params.add(new ParamEntry(key, b));
    return this;
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
    RpcPacketBuilder rpcPacketBuilder = new RpcPacketBuilder(sql, params);
    ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPacket();
//    RpcPacketBuildersave rpcPacketBuilder = new RpcPacketBuildersave();
//    ByteBuffer rpcPacket = rpcPacketBuilder.buildRpcPayload("Michael", "Thomas", "mt@mt.com", 12);
    return client.rpcAsync(rpcPacket);
  }
}