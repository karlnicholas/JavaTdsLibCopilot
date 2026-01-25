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

  PreparedRpcQuery bindShort(String param, Short value);
  PreparedRpcQuery bindInteger(String param, Integer value);
  PreparedRpcQuery bindLong(String param, Long value);
  PreparedRpcQuery bindString(String param, String value);
  PreparedRpcQuery bindBytes(String param, byte[] value);
  PreparedRpcQuery bindBoolean(String param, Boolean value);
  PreparedRpcQuery bindSQLXML(String param, SQLXML xml);
  PreparedRpcQuery bindNClob(String param, NClob nclob);
  PreparedRpcQuery bindClob(String param, Clob clob);
  PreparedRpcQuery bindBlob(String param, Blob blob);
  PreparedRpcQuery bindTimestamp(String param, Timestamp ts);
  PreparedRpcQuery bindTime(String param, Time time);
  PreparedRpcQuery bindDate(String param, Date date);
  PreparedRpcQuery bindBigDecimal(String param, BigDecimal bd);
  PreparedRpcQuery bindDouble(String param, Double d);
  PreparedRpcQuery bindFloat(String param, Float f);
  PreparedRpcQuery bindByte(String param, Byte b);

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

  default void bindParam(String param, Object value) {
    if (value == null) {
      throw new IllegalArgumentException(
              "Null value at param " + param +
                      " — use bindNull(param, jdbcType) or explicit type for nulls");
// If your TdsClient supports generic null, you could call:
// bindNull(index, java.sql.Types.OTHER);
    }
// Pattern matching instanceof (final in Java 17)
    if (value instanceof Byte b) {
      bindByte(param, b);
    } else if (value instanceof Short s) {
      bindShort(param, s);
    } else if (value instanceof Integer i) {
      bindInteger(param, i);
    } else if (value instanceof Long l) {
      bindLong(param, l);
    } else if (value instanceof Float f) {
      bindFloat(param, f);
    } else if (value instanceof Double d) {
      bindDouble(param, d);
    } else if (value instanceof java.math.BigDecimal bd) {
      bindBigDecimal(param, bd);
    } else if (value instanceof Boolean bool) {
      bindBoolean(param, bool);
    } else if (value instanceof String str) {
      bindString(param, str);  // fallback; consider bindNString if needed
    } else if (value instanceof java.sql.Date date) {
      bindDate(param, date);
    } else if (value instanceof java.sql.Time time) {
      bindTime(param, time);
    } else if (value instanceof java.sql.Timestamp ts) {
      bindTimestamp(param, ts);
    } else if (value instanceof byte[] bytes) {
      bindBytes(param, bytes);
    } else if (value instanceof java.sql.Blob blob) {
      bindBlob(param, blob);
    } else if (value instanceof java.sql.Clob clob) {
      bindClob(param, clob);
    } else if (value instanceof java.sql.NClob nclob) {
      bindNClob(param, nclob);
    } else if (value instanceof java.sql.SQLXML xml) {
      bindSQLXML(param, xml);
    } else {
      throw new IllegalArgumentException(
              "Unsupported automatic bind type: " + value.getClass().getName() +
                      " (value = " + value + "). " +
                      "Use explicit bindXxx(...) or bindObject(index, value, jdbcType)");
    }
  }
// Optional future extensions
// Flow.Publisher<Integer> executeUpdate();           // for INSERT/UPDATE/DELETE
// Flow.Publisher<OutputParam> executeWithOutputs();  // for stored procs with OUTPUT
}