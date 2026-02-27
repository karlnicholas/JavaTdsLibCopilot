package org.tdslib.javatdslib.decode;

import org.tdslib.javatdslib.TdsType;
import java.nio.charset.Charset;

public interface ResultDecoder {
  boolean canDecode(TdsType tdsType);

  <T> T decode(byte[] data, TdsType tdsType, Class<T> targetType, int scale, Charset varcharCharset);
}