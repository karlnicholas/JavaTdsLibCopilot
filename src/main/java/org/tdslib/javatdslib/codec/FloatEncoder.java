package org.tdslib.javatdslib.codec;

import java.nio.ByteBuffer;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.query.rpc.ParamEntry;
import org.tdslib.javatdslib.query.rpc.ParameterCodec;
import org.tdslib.javatdslib.query.rpc.RpcEncodingContext;

/**
 * Codec for encoding floating-point values (Float, Double) into TDS REAL/FLOAT format.
 */
public class FloatEncoder implements ParameterCodec {

  @Override
  public boolean canEncode(ParamEntry entry) {
    TdsType type = entry.key().type();
    return type == TdsType.FLT4 || type == TdsType.REAL
        || type == TdsType.FLT8 || type == TdsType.FLTN;
  }

  @Override
  public String getSqlTypeDeclaration(ParamEntry entry) {
    TdsType type = entry.key().type();
    Object val = entry.value().getValue();

    if (type == TdsType.FLT4 || type == TdsType.REAL || val instanceof Float) {
      return "real";
    }
    return "float";
  }

  @Override
  public void writeTypeInfo(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    buf.put((byte) TdsType.FLTN.byteVal); // Always send as variable length FLTN

    TdsType type = entry.key().type();
    Object val = entry.value().getValue();

    if (type == TdsType.FLT4 || type == TdsType.REAL || val instanceof Float) {
      buf.put((byte) 4);
    } else {
      buf.put((byte) 8);
    }
  }

  @Override
  public void writeValue(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    Object value = entry.value().getValue();
    if (value == null) {
      buf.put((byte) 0); // Null byte length
      return;
    }

    if (value instanceof Float) {
      buf.put((byte) 4);
      buf.putFloat((Float) value);
    } else {
      buf.put((byte) 8);
      buf.putDouble(((Number) value).doubleValue());
    }
  }
}