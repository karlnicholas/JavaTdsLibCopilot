package org.tdslib.javatdslib.decode;

import java.nio.charset.Charset;
import org.tdslib.javatdslib.TdsType;

/**
 * Interface for decoding TDS byte arrays into Java objects.
 */
public interface ResultDecoder {

  /**
   * Checks if this decoder supports the given TDS type.
   *
   * @param tdsType the TDS type to check
   * @return true if supported, false otherwise
   */
  boolean canDecode(TdsType tdsType);

  /**
   * Decodes the given data into the target type.
   *
   * @param data           the raw byte data
   * @param tdsType        the TDS type of the data
   * @param targetType     the desired Java type
   * @param scale          the scale (for numeric/decimal types)
   * @param varcharCharset the charset to use for string decoding
   * @param <T>            the type of the result
   * @return the decoded object
   */
  <T> T decode(byte[] data, TdsType tdsType, Class<T> targetType, int scale,
               Charset varcharCharset);
}