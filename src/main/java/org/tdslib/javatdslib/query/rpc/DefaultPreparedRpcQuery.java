package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.RowWithMetadata;
import org.tdslib.javatdslib.TdsClient;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

public final class DefaultPreparedRpcQuery implements PreparedRpcQuery {

  private final String sql;

  private final List<ParamEntry> params = new ArrayList<>();

  private int fetchSize = -1;
  private Duration timeout = null;

  public DefaultPreparedRpcQuery(String sql) {
    this.sql = sql.trim();
  }

  @Override
  public PreparedRpcQuery bind(String name, Object value) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingKind.NAMED, name, -1, order);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bind(int index, Object value) {
    int order = params.size() + 1;
    BindingKey key = new BindingKey(BindingKind.INDEXED, null, index, order);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery bind(Object value) {
    int order = params.size() + 1;
    // For IMPLIED we assign a sequential index based on current list size
    int autoIndex = params.size();   // 0-based
    BindingKey key = new BindingKey(BindingKind.IMPLIED, null, autoIndex, order);
    params.add(new ParamEntry(key, value));
    return this;
  }

  @Override
  public PreparedRpcQuery fetchSize(int rows) {
    this.fetchSize = rows;
    return this;
  }

  @Override
  public PreparedRpcQuery timeout(Duration t) {
    this.timeout = t;
    return this;
  }

  @Override
  public Flow.Publisher<RowWithMetadata> execute(TdsClient client) {
    return createTdsPublisher(client, sql, params, fetchSize, timeout);
  }

  private Flow.Publisher<RowWithMetadata> createTdsPublisher(
      TdsClient client,
      String sql,
      List<ParamEntry> params,
      int fetchSize,
      Duration timeout) {

    // Example logic you can adapt:
    // Iterate in bind order (natural list order)
    for (ParamEntry entry : params) {
      BindingKey bk = entry.key();
      Object value = entry.value();

      switch (bk.kind()) {
        case NAMED:
          // send with name = bk.name() (length > 0)
          break;
        case INDEXED:
        case IMPLIED:
          // send unnamed (length = 0)
          // you could use bk.index() for logging or if you want to sort/reorder
          break;
      }
      // write type info + value ...
    }


    // return your actual publisher implementation
    ByteBuffer rpcBuffer = new RpcPacketBuilder().buildRpcPayload(
        "Michael", "Brown", "mb@m.com", 12);
    return client.rpcAsync(rpcBuffer);
  }
}