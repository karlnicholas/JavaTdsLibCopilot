package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.RowWithMetadata;
import org.tdslib.javatdslib.TdsClient;

import java.util.Map;
import java.util.concurrent.Flow;

/**
 * Fluent builder for preparing and executing an RPC-style query (prepared statement)
 * against a SQL Server / TDS connection.
 *
 * <p>Supports two parameter styles:
 * <ul>
 *   <li>Positional: using {@code ?} placeholders → bind via {@link #bind(int, Object)} or varargs</li>
 *   <li>Named: using {@code @name} or {@code :name} → bind via {@link #bind(String, Object)}</li>
 * </ul>
 *
 * The style is auto-detected from the SQL string, but mixing styles in the same query
 * will typically throw an exception.
 */
public interface PreparedRpcQuery {

  /**
   * Binds a parameter — flexible overload that works for both styles.
   * <ul>
   *   <li>If positional style (?): tries to parse the key as an integer index</li>
   *   <li>Otherwise: treats as named parameter (e.g. "@id", "id", ":status")</li>
   * </ul>
   */
  PreparedRpcQuery bind(String key, Object value);

  /**
   * Binds a parameter — implied positional
   * <ul>
   *   <li>If positional style (?): tries to parse the key as an integer index</li>
   *   <li>Otherwise: treats as named parameter (e.g. "@id", "id", ":status")</li>
   * </ul>
   */
  PreparedRpcQuery bind(Object value);

  /**
   * Binds a positional parameter (0-based index recommended).
   * Mainly useful (and type-safe) when using {@code ?} placeholders.
   */
  PreparedRpcQuery bind(int index, Object value);

  /**
   * Optional: explicit named binding (ignores auto-detection).
   * Useful if you want to force named style even with ? in SQL.
   */
//  PreparedRpcQuery bindNamed(String name, Object value);

  /**
   * Optional: set fetch size (row batching hint for TDS STREAM).
   * Negative or zero = default / unlimited.
   */
  PreparedRpcQuery fetchSize(int rows);

  /**
   * Optional: set command timeout.
   */
  PreparedRpcQuery timeout(java.time.Duration timeout);

  /**
   * Executes the prepared RPC and returns a reactive publisher of result rows.
   *
   * <p>After this call, further bind() calls on this instance are usually invalid.
   *
   * @return Flow.Publisher that emits rows as they arrive from the TDS stream
   */
  Flow.Publisher<RowWithMetadata> execute(TdsClient client);

  /**
   * Convenience: execute with positional arguments in order (JDBC-style).
   * Automatically calls {@link #bind(int, Object)} for each value (0-based).
   */
  default Flow.Publisher<RowWithMetadata> execute(TdsClient client, Object... values) {
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        bind(i, values[i]);
      }
    }
    return execute(client);
  }

  /**
   * Convenience: execute with named parameters (map keys should match @param names
   * without the @ prefix, or include it — implementation normalizes).
   */
  default Flow.Publisher<RowWithMetadata> execute(TdsClient client, Map<String, ?> params) {
    if (params != null) {
      params.forEach(this::bind);
    }
    return execute(client);
  }

  // Optional future extensions
  // Flow.Publisher<Integer> executeUpdate();           // for INSERT/UPDATE/DELETE
  // Flow.Publisher<OutputParam> executeWithOutputs();  // for stored procs with OUTPUT
}