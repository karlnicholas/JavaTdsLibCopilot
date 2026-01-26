package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.RowWithMetadata;
import org.tdslib.javatdslib.TdsClient;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.Flow;
/**
 Fluent builder for preparing and executing an RPC-style query (prepared statement)
 against a SQL Server / TDS connection.
 */
public interface PreparedRpcQuery {

  PreparedRpcQuery bind(String param, Object value);

  PreparedRpcQuery bindNull(String param, Class<?> type);

  /**
   Optional: set fetch size (row batching hint for TDS STREAM).
   Negative or zero = default / unlimited.
   */
  PreparedRpcQuery fetchSize(int rows);

  /**
   Optional: set command timeout.
   */
  PreparedRpcQuery timeout(java.time.Duration timeout);

  /**
   Executes the prepared RPC and returns a reactive publisher of result rows.
   After this call, further bind() calls on this instance are usually invalid.
   @return Flow.Publisher that emits rows as they arrive from the TDS stream
   */
  Flow.Publisher<RowWithMetadata> execute(TdsClient client);
  Flow.Publisher<RowWithMetadata> executeQuery(TdsClient client);
  Flow.Publisher<RowWithMetadata> executeUpdate(TdsClient client);

  /**
   Convenience: execute with named parameters (map keys should match @param names
   without the @ prefix, or include it — implementation normalizes).
   */
  default Flow.Publisher<RowWithMetadata> execute(TdsClient client, Map<String, ?> params) {
    if (params != null) {
      params.forEach(this::bindParam);
    }
    return execute(client);
  }

  default Flow.Publisher<RowWithMetadata> executeQuery(TdsClient client, Map<String, ?> params) {
    if (params != null) {
      params.forEach(this::bindParam);
    }
    return executeQuery(client);
  }

  default Flow.Publisher<RowWithMetadata> executeUpdate(TdsClient client, Map<String, ?> params) {
    if (params != null) {
      params.forEach(this::bindParam);
    }
    return executeUpdate(client);
  }

  default void bindParam(String param, Object value) {
    if (value == null) {
      throw new IllegalArgumentException(
              "Null value at param " + param +
                      " — use bindNull(param, jdbcType) or explicit type for nulls");
// If your TdsClient supports generic null, you could call:
// bindNull(index, java.sql.Types.OTHER);
    }
    bind(param, value);
// Pattern matching instanceof (final in Java 17)
  }
// Optional future extensions
// Flow.Publisher<Integer> executeUpdate();           // for INSERT/UPDATE/DELETE
// Flow.Publisher<OutputParam> executeWithOutputs();  // for stored procs with OUTPUT
}