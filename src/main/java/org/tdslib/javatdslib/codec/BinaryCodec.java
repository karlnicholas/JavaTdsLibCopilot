package org.tdslib.javatdslib.codec;

import java.nio.ByteBuffer;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.query.rpc.ParamEntry;
import org.tdslib.javatdslib.query.rpc.ParameterCodec;
import org.tdslib.javatdslib.query.rpc.RpcEncodingContext;

/**
 * Codec for encoding binary data types (BINARY, VARBINARY, IMAGE) into TDS format.
 */
public class BinaryCodec implements ParameterCodec {

  @Override
  public boolean canEncode(ParamEntry entry) {
    TdsType type = entry.key().type();
    return type == TdsType.BIGVARBIN || type == TdsType.BIGBINARY
        || type == TdsType.VARBINARY || type == TdsType.BINARY || type == TdsType.IMAGE;
  }

  @Override
  public String getSqlTypeDeclaration(ParamEntry entry) {
    byte[] data = getBytes(entry.value().getValue());
    if (data != null && data.length > 8000) {
      return "varbinary(max)";
    }
    return "varbinary(8000)";
  }

  @Override
  public void writeTypeInfo(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    buf.put((byte) TdsType.BIGVARBIN.byteVal); // Always send as BIGVARBIN (0xA5)

    byte[] data = getBytes(entry.value().getValue());
    if (data != null && data.length > 8000) {
      buf.putShort((short) -1); // 0xFFFF for PLP max length
    } else {
      buf.putShort((short) 8000);
    }
  }

  @Override
  public void writeValue(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    byte[] data = getBytes(entry.value().getValue());
    if (data == null) {
      buf.putShort((short) 0xFFFF);
      return;
    }

    if (data.length > 8000) {
      // PLP Chunking for varbinary(max)
      buf.putLong(data.length);
      buf.putInt(data.length);
      buf.put(data);
      buf.putInt(0);
    } else {
      buf.putShort((short) data.length);
      buf.put(data);
    }
  }

  private byte[] getBytes(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    if (value instanceof ByteBuffer bb) {
      byte[] b = new byte[bb.remaining()];
      bb.duplicate().get(b);
      return b;
    }
    return null;
  }
}