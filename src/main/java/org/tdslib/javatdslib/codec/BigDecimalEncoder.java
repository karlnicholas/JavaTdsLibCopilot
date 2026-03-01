package org.tdslib.javatdslib.codec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.ParamEntry;
import org.tdslib.javatdslib.protocol.rpc.ParameterCodec;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;

/**
 * Codec for encoding BigDecimal values into TDS DECIMAL/NUMERIC format.
 */
public class BigDecimalEncoder implements ParameterCodec {

  @Override
  public boolean canEncode(ParamEntry entry) {
    TdsType type = entry.key().type();
    return type == TdsType.DECIMAL || type == TdsType.NUMERIC
        || type == TdsType.DECIMALN || type == TdsType.NUMERICN;
  }

  @Override
  public String getSqlTypeDeclaration(ParamEntry entry) {
    int scale = getDecimalScale(entry);
    return String.format("decimal(38,%d)", scale);
  }

  @Override
  public void writeTypeInfo(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    buf.put((byte) TdsType.DECIMALN.byteVal); // Always send as variable length DECIMALN
    buf.put((byte) 17);                       // Max length for precision 38 is 17 bytes
    buf.put((byte) 38);                       // Precision
    buf.put((byte) getDecimalScale(entry));   // Scale
  }

  @Override
  public void writeValue(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    Object value = entry.value().getValue();
    if (value == null) {
      buf.put((byte) 0); // Null byte length
      return;
    }

    BigDecimal bd = (BigDecimal) value;
    int scale = getDecimalScale(entry);

    byte[] decBytes = convertToDecimalBytes(bd, scale);

    // Write the length of the upcoming decimal byte array
    buf.put((byte) decBytes.length);
    buf.put(decBytes);
  }

  private int getDecimalScale(ParamEntry entry) {
    Object value = entry.value().getValue();
    if (value instanceof BigDecimal bd) {
      return Math.max(0, Math.min(38, bd.scale()));
    }
    return 0;
  }

  /**
   * Converts a Java BigDecimal into the MS-TDS proprietary binary format:
   * 1 byte sign (1 = positive, 0 = negative) followed by little-endian unscaled value.
   */
  private byte[] convertToDecimalBytes(BigDecimal bd, int scale) {
    BigDecimal scaledBd = bd.setScale(scale, RoundingMode.HALF_UP);
    BigInteger unscaled = scaledBd.unscaledValue();
    byte[] bytes = unscaled.toByteArray();

    byte sign = (byte) (unscaled.signum() >= 0 ? 1 : 0);
    byte[] res = new byte[bytes.length + 1];
    res[0] = sign;

    // Reverse the big-endian BigInteger bytes into little-endian format
    for (int i = 0; i < bytes.length; i++) {
      res[i + 1] = bytes[bytes.length - 1 - i];
    }

    return res;
  }
}