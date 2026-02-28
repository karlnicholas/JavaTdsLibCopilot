package org.tdslib.javatdslib.decode;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.tdslib.javatdslib.TdsType;

/**
 * Decodes string data types (CHAR, VARCHAR, NCHAR, NVARCHAR, TEXT, NTEXT, XML) from TDS bytes.
 */
public class StringDecoder implements ResultDecoder {

  @Override
  public boolean canDecode(TdsType tdsType) {
    return tdsType == TdsType.BIGCHAR
        || tdsType == TdsType.BIGVARCHR
        || tdsType == TdsType.VARCHAR
        || tdsType == TdsType.CHAR
        || tdsType == TdsType.TEXT
        || tdsType == TdsType.NVARCHAR
        || tdsType == TdsType.NCHAR
        || tdsType == TdsType.NTEXT
        || tdsType == TdsType.XML;
  }

  @Override
  public <T> T decode(byte[] data, TdsType tdsType, Class<T> targetType, int scale,
                      Charset varcharCharset) {
    String result;
    if (isNationalChar(tdsType)) {
      result = new String(data, StandardCharsets.UTF_16LE);
    } else {
      result = new String(data, varcharCharset);
    }
    return targetType.cast(result);
  }

  private boolean isNationalChar(TdsType tdsType) {
    return tdsType == TdsType.NVARCHAR
        || tdsType == TdsType.NCHAR
        || tdsType == TdsType.NTEXT
        || tdsType == TdsType.XML;
  }
}