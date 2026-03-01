package org.tdslib.javatdslib.codec;

import java.nio.ByteBuffer;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.ParamEntry;
import org.tdslib.javatdslib.protocol.rpc.ParameterEncoder;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;

/**
 * Codec for encoding Boolean values into TDS BIT/BITN format.
 */
public class BooleanEncoder implements ParameterEncoder {

  @Override
  public boolean canEncode(ParamEntry entry) {
    TdsType type = entry.key().type();
    return type == TdsType.BIT || type == TdsType.BITN;
  }

  @Override
  public String getSqlTypeDeclaration(ParamEntry entry) {
    return "bit";
  }

  @Override
  public void writeTypeInfo(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    // Always send as variable length BITN (0x68) for RPC
    buf.put((byte) TdsType.BITN.byteVal);
    buf.put((byte) 1); // Maximum length is 1 byte
  }

  @Override
  public void writeValue(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    Object value = entry.value().getValue();
    if (value == null) {
      buf.put((byte) 0); // Null byte length
      return;
    }

    buf.put((byte) 1); // Data length is 1

    boolean b = false;
    if (value instanceof Boolean) {
      b = (Boolean) value;
    } else if (value instanceof Number) {
      b = ((Number) value).intValue() != 0;
    }

    buf.put((byte) (b ? 1 : 0));
  }
}