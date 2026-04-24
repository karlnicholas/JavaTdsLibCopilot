package org.tdslib.javatdslib.protocol.rpc;

import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.protocol.TdsParameter;
import java.nio.ByteBuffer;

/**
 * Codec for encoding Large Object (LOB) data types into a reactive stream of TDS format chunks.
 */
public interface StreamingParameterEncoder {

  boolean canEncode(TdsParameter entry);

  String getSqlTypeDeclaration(TdsParameter entry);

  void writeTypeInfo(ByteBuffer buf, TdsParameter entry, RpcEncodingContext context);

  /**
   * Instead of synchronously writing to a buffer, this returns a reactive stream
   * of ByteBuffers, formatted as TDS PLP (Partially Length-Prefixed) chunks.
   */
  Publisher<ByteBuffer> streamValue(TdsParameter entry, RpcEncodingContext context);
}