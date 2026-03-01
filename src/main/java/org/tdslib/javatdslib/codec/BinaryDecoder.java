package org.tdslib.javatdslib.codec;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.tdslib.javatdslib.protocol.TdsType;

/**
 * Decodes binary data types (BINARY, VARBINARY, IMAGE) from TDS byte arrays.
 */
public class BinaryDecoder implements ResultDecoder {

  @Override
  public boolean canDecode(TdsType tdsType) {
    return tdsType == TdsType.BIGVARBIN
        || tdsType == TdsType.VARBINARY
        || tdsType == TdsType.BINARY
        || tdsType == TdsType.IMAGE
        || tdsType == TdsType.BIGBINARY;
  }

  @Override
  public <T> T decode(byte[] data, TdsType tdsType, Class<T> targetType, int scale,
                      Charset varcharCharset) {
    if (targetType == ByteBuffer.class) {
      return targetType.cast(ByteBuffer.wrap(data));
    }
    return targetType.cast(data);
  }
}