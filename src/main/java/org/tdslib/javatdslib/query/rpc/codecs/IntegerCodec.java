package org.tdslib.javatdslib.query.rpc.codecs;

import java.nio.ByteBuffer;
import org.tdslib.javatdslib.TdsType;
import org.tdslib.javatdslib.query.rpc.ParamEntry;
import org.tdslib.javatdslib.query.rpc.ParameterCodec;
import org.tdslib.javatdslib.query.rpc.RpcEncodingContext;

/**
 * Codec for encoding integer values (Byte, Short, Integer, Long) into TDS INT format.
 */
public class IntegerCodec implements ParameterCodec {

  @Override
  public boolean canEncode(ParamEntry entry) {
    TdsType type = entry.key().type();
    return type == TdsType.INT1 || type == TdsType.INT2
        || type == TdsType.INT4 || type == TdsType.INT8 || type == TdsType.INTN;
  }

  @Override
  public String getSqlTypeDeclaration(ParamEntry entry) {
    TdsType type = entry.key().type();

    if (type == TdsType.INT1) {
      return "tinyint";
    }
    if (type == TdsType.INT2) {
      return "smallint";
    }
    if (type == TdsType.INT4) {
      return "int";
    }
    if (type == TdsType.INT8) {
      return "bigint";
    }

    if (type == TdsType.INTN) {
      Object value = entry.value().getValue();
      if (value instanceof Long) {
        return "bigint";
      }
      if (value instanceof Short) {
        return "smallint";
      }
      if (value instanceof Byte) {
        return "tinyint";
      }
      return "int";
    }
    return "int";
  }

  @Override
  public void writeTypeInfo(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    buf.put((byte) TdsType.INTN.byteVal); // Always send as variable length INTN

    TdsType type = entry.key().type();
    Object val = entry.value().getValue();
    byte maxLen = 4;

    if (type == TdsType.INT1 || val instanceof Byte) {
      maxLen = 1;
    } else if (type == TdsType.INT2 || val instanceof Short) {
      maxLen = 2;
    } else if (type == TdsType.INT8 || val instanceof Long) {
      maxLen = 8;
    }

    buf.put(maxLen);
  }

  @Override
  public void writeValue(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    Object value = entry.value().getValue();
    if (value == null) {
      buf.put((byte) 0); // Null byte length
      return;
    }

    if (value instanceof Long) {
      buf.put((byte) 8);
      buf.putLong((Long) value);
    } else if (value instanceof Integer) {
      buf.put((byte) 4);
      buf.putInt((Integer) value);
    } else if (value instanceof Short) {
      buf.put((byte) 2);
      buf.putShort((Short) value);
    } else if (value instanceof Byte) {
      buf.put((byte) 1);
      buf.put((Byte) value);
    }
  }
}