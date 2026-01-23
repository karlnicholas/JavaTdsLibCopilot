package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.RowWithMetadata;
import org.tdslib.javatdslib.TdsClient;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.Flow;
/**

 Fluent builder for preparing and executing an RPC-style query (prepared statement)
 against a SQL Server / TDS connection.

 Supports two parameter styles:



 Positional: using {@code ?} placeholders → bind via {@link #bind(int, Object)} or varargs

 Named: using {@code @name} or {@code :name} → bind via {@link #bind(String, Object)}



 The style is auto-detected from the SQL string, but mixing styles in the same query
 will typically throw an exception.
 */
public interface PreparedRpcQuery {

  /**

   Binds a parameter — flexible overload that works for both styles.


   If positional style (?): tries to parse the key as an integer index

   Otherwise: treats as named parameter (e.g. "@id", "id", ":status")



   */
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

   Binds a parameter — implied positional


   If positional style (?): tries to parse the key as an integer index

   Otherwise: treats as named parameter (e.g. "@id", "id", ":status")



   */
  PreparedRpcQuery bindShort(Short value);
  PreparedRpcQuery bindInteger(Integer value);
  PreparedRpcQuery bindLong(Long value);
  PreparedRpcQuery bindString(String value);
  PreparedRpcQuery bindBytes(byte[] value);
  PreparedRpcQuery bindBoolean(Boolean value);
  PreparedRpcQuery bindSQLXML(SQLXML xml);
  PreparedRpcQuery bindNClob(NClob nclob);
  PreparedRpcQuery bindClob(Clob clob);
  PreparedRpcQuery bindBlob(Blob blob);
  PreparedRpcQuery bindTimestamp(Timestamp ts);
  PreparedRpcQuery bindTime(Time time);
  PreparedRpcQuery bindDate(Date date);
  PreparedRpcQuery bindBigDecimal(BigDecimal bd);
  PreparedRpcQuery bindDouble(Double d);
  PreparedRpcQuery bindFloat(Float f);
  PreparedRpcQuery bindByte(Byte b);
  /**

   Binds a positional parameter (0-based index recommended).
   Mainly useful (and type-safe) when using {@code ?} placeholders.
   */
  PreparedRpcQuery bindShort(int index, Short value);
  PreparedRpcQuery bindInteger(int index, Integer value);
  PreparedRpcQuery bindLong(int index, Long value);
  PreparedRpcQuery bindString(int index, String value);
  PreparedRpcQuery bindBytes(int index, byte[] value);
  PreparedRpcQuery bindBoolean(int index, Boolean value);
  PreparedRpcQuery bindSQLXML(int index, SQLXML xml);
  PreparedRpcQuery bindNClob(int index, NClob nclob);
  PreparedRpcQuery bindClob(int index, Clob clob);
  PreparedRpcQuery bindBlob(int index, Blob blob);
  PreparedRpcQuery bindTimestamp(int index, Timestamp ts);
  PreparedRpcQuery bindTime(int index, Time time);
  PreparedRpcQuery bindDate(int index, Date date);
  PreparedRpcQuery bindBigDecimal(int index, BigDecimal bd);
  PreparedRpcQuery bindDouble(int index, Double d);
  PreparedRpcQuery bindFloat(int index, Float f);
  PreparedRpcQuery bindByte(int index, Byte b);

/**

 Optional: explicit named binding (ignores auto-detection).
 Useful if you want to force named style even with ? in SQL.
 */
//  PreparedRpcQuery bindNamed(String name, Object value);

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

   Convenience: execute with positional arguments in order (JDBC-style).
   */
  default Flow.Publisher<RowWithMetadata> execute(TdsClient client, Object... values) {
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        bindIndex(i + 1, values[i]);  // 1-based index
      }
    }
    return execute(client);
  }

  default void bindIndex(int index, Object value) {
    if (value == null) {
      throw new IllegalArgumentException(
              "Null value at index " + index +
                      " — use bindNull(index, jdbcType) or explicit type for nulls");
// If your TdsClient supports generic null, you could call:
// bindNull(index, java.sql.Types.OTHER);
    }
// Pattern matching instanceof (final in Java 17)
    if (value instanceof Byte b) {
      bindByte(index, b);
    } else if (value instanceof Short s) {
      bindShort(index, s);
    } else if (value instanceof Integer i) {
      bindInteger(index, i);
    } else if (value instanceof Long l) {
      bindLong(index, l);
    } else if (value instanceof Float f) {
      bindFloat(index, f);
    } else if (value instanceof Double d) {
      bindDouble(index, d);
    } else if (value instanceof java.math.BigDecimal bd) {
      bindBigDecimal(index, bd);
    } else if (value instanceof Boolean bool) {
      bindBoolean(index, bool);
    } else if (value instanceof String str) {
      bindString(index, str);  // fallback; consider bindNString if needed
    } else if (value instanceof java.sql.Date date) {
      bindDate(index, date);
    } else if (value instanceof java.sql.Time time) {
      bindTime(index, time);
    } else if (value instanceof java.sql.Timestamp ts) {
      bindTimestamp(index, ts);
    } else if (value instanceof byte[] bytes) {
      bindBytes(index, bytes);
    } else if (value instanceof java.sql.Blob blob) {
      bindBlob(index, blob);
    } else if (value instanceof java.sql.Clob clob) {
      bindClob(index, clob);
    } else if (value instanceof java.sql.NClob nclob) {
      bindNClob(index, nclob);
    } else if (value instanceof java.sql.SQLXML xml) {
      bindSQLXML(index, xml);
    } else {
      throw new IllegalArgumentException(
              "Unsupported automatic bind type: " + value.getClass().getName() +
                      " (value = " + value + "). " +
                      "Use explicit bindXxx(...) or bindObject(index, value, jdbcType)");
    }
  }
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