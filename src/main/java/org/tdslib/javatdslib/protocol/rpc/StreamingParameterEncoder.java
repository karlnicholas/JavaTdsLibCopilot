package org.tdslib.javatdslib.protocol.rpc;

import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.protocol.TdsParameter;
import java.nio.ByteBuffer;

/**
 * Codec for encoding Large Object (LOB) data types into a reactive stream of TDS format chunks.
 */
public interface StreamingParameterEncoder {

  /**
   * Checks if this encoder can handle the given parameter.
   *
   * @param entry The parameter to check.
   * @return {@code true} if this encoder can handle the parameter, {@code false} otherwise.
   */
  boolean canEncode(TdsParameter entry);

  /**
   * Gets the SQL type declaration for the parameter (e.g., "nvarchar(max)").
   *
   * @param entry The parameter for which to get the SQL type.
   * @return The SQL type declaration string.
   */
  String getSqlTypeDeclaration(TdsParameter entry);

  /**
   * Writes the TDS type information for the parameter into the buffer.
   *
   * @param buf The buffer to write to.
   * @param entry The parameter whose type info should be written.
   * @param context The encoding context.
   */
  void writeTypeInfo(ByteBuffer buf, TdsParameter entry, RpcEncodingContext context);

  /**
   * Instead of synchronously writing to a buffer, this returns a reactive stream
   * of ByteBuffers, formatted as TDS PLP (Partially Length-Prefixed) chunks.
   */
  Publisher<ByteBuffer> streamValue(TdsParameter entry, RpcEncodingContext context);
}