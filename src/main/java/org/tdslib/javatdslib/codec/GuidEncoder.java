package org.tdslib.javatdslib.codec;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.ParamEntry;
import org.tdslib.javatdslib.protocol.rpc.ParameterEncoder;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;

/**
 * Codec for encoding UUID values into TDS uniqueidentifier format.
 */
public class GuidEncoder implements ParameterEncoder {

  @Override
  public boolean canEncode(ParamEntry entry) {
    return entry.key().type() == TdsType.GUID;
  }

  @Override
  public String getSqlTypeDeclaration(ParamEntry entry) {
    return "uniqueidentifier";
  }

  @Override
  public void writeTypeInfo(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    buf.put((byte) TdsType.GUID.byteVal);
    buf.put((byte) 16); // Always 16 bytes length indicator
  }

  @Override
  public void writeValue(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    Object value = entry.value().getValue();
    if (value == null) {
      buf.put((byte) 0);
      return;
    }

    buf.put((byte) 16); // Payload length
    UUID uuid = value instanceof UUID ? (UUID) value : UUID.fromString(value.toString());

    long msb = uuid.getMostSignificantBits();
    long lsb = uuid.getLeastSignificantBits();

    // MS-SQL requires the first 3 groups of the GUID to be little-endian, and the last 2 big-endian
    buf.putInt(Integer.reverseBytes((int) (msb >>> 32)));
    buf.putShort(Short.reverseBytes((short) (msb >>> 16)));
    buf.putShort(Short.reverseBytes((short) msb));

    buf.put((byte) (lsb >>> 56));
    buf.put((byte) (lsb >>> 48));
    buf.put((byte) (lsb >>> 40));
    buf.put((byte) (lsb >>> 32));
    buf.put((byte) (lsb >>> 24));
    buf.put((byte) (lsb >>> 16));
    buf.put((byte) (lsb >>> 8));
    buf.put((byte) lsb);
  }
}