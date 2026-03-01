package org.tdslib.javatdslib.codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.UUID;
import org.tdslib.javatdslib.protocol.TdsType;

/**
 * Decodes GUID (UUID) data types from TDS byte arrays.
 */
public class GuidDecoder implements ResultDecoder {

  @Override
  public boolean canDecode(TdsType tdsType) {
    return tdsType == TdsType.GUID;
  }

  @Override
  public <T> T decode(byte[] data, TdsType tdsType, Class<T> targetType, int scale,
                      Charset varcharCharset) {
    if (targetType == UUID.class) {
      ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
      long msb = bb.getLong();
      long lsb = bb.getLong();
      return targetType.cast(new UUID(msb, lsb));
    }
    return targetType.cast(convertBytesToHex(data));
  }

  private String convertBytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}