package org.tdslib.javatdslib.codec;

import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.ParameterEncoder;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;

import java.nio.ByteBuffer;

/**
 * Codec for encoding floating-point values (Float, Double) into TDS REAL/FLOAT format.
 */
public class FloatEncoder implements ParameterEncoder {

  @Override
  public boolean canEncode(TdsParameter entry) {
    TdsType type = entry.type();
    return type == TdsType.FLT4 || type == TdsType.REAL
        || type == TdsType.FLT8 || type == TdsType.FLTN;
  }

  @Override
  public String getSqlTypeDeclaration(TdsParameter entry) {
    TdsType type = entry.type();
    Object val = entry.value();

    if (type == TdsType.FLT4 || type == TdsType.REAL || val instanceof Float) {
      return "real";
    }
    return "float";
  }

  @Override
  public void writeTypeInfo(ByteBuffer buf, TdsParameter entry, RpcEncodingContext context) {
    buf.put((byte) TdsType.FLTN.byteVal); // Always send as variable length FLTN

    TdsType type = entry.type();
    Object val = entry.value();

    if (type == TdsType.FLT4 || type == TdsType.REAL || val instanceof Float) {
      buf.put((byte) 4);
    } else {
      buf.put((byte) 8);
    }
  }

  @Override
  public void writeValue(ByteBuffer buf, TdsParameter entry, RpcEncodingContext context) {
    Object value = entry.value();
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