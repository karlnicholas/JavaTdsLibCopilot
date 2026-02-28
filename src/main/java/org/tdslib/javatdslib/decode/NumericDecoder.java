package org.tdslib.javatdslib.decode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import org.tdslib.javatdslib.TdsType;

/**
 * Decodes numeric data types (integers, floats, decimals, money) from TDS byte arrays.
 */
public class NumericDecoder implements ResultDecoder {

  @Override
  public boolean canDecode(TdsType tdsType) {
    switch (tdsType) {
      case INT1:
      case INT2:
      case INT4:
      case INT8:
      case INTN:
      case FLT4:
      case FLT8:
      case FLTN:
      case BIT:
      case BITN:
      case NUMERIC:
      case DECIMAL:
      case NUMERICN:
      case DECIMALN:
      case MONEY:
      case MONEYN:
      case SMALLMONEY:
        return true;
      default:
        return false;
    }
  }

  @Override
  public <T> T decode(byte[] data, TdsType tdsType, Class<T> targetType, int scale,
                      Charset varcharCharset) {
    switch (tdsType) {
      case INT1:
        // FIX: Mask with 0xFF to treat SQL Server TINYINT as Unsigned
        return convertSimple(data[0] & 0xFF, targetType);
      case INT2:
        return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort(),
            targetType);
      case INT4:
        return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt(),
            targetType);
      case INT8:
        return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong(),
            targetType);
      case INTN:
        if (data.length == 1) {
          return convertSimple(data[0] & 0xFF, targetType); // FIX: Unsigned mask
        }
        if (data.length == 2) {
          return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort(),
              targetType);
        }
        if (data.length == 4) {
          return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt(),
              targetType);
        }
        if (data.length == 8) {
          return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong(),
              targetType);
        }
        throw new IllegalStateException("Unexpected INTN length");

      case FLT4:
        return targetType.cast(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getFloat());

      case FLTN:
        if (data.length == 4) {
          return targetType.cast(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getFloat());
        }
        // Explicitly handle the 8-byte FLTN scenario instead of falling through
        double fltnDouble = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getDouble();
        if (targetType == Float.class) {
          return targetType.cast((float) fltnDouble);
        }
        return targetType.cast(fltnDouble);

      case FLT8:
        double flt8Double = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getDouble();
        if (targetType == Float.class) {
          return targetType.cast((float) flt8Double);
        }
        return targetType.cast(flt8Double);

      case BIT:
      case BITN:
        boolean boolVal = data[0] != 0;
        if (targetType == Boolean.class) {
          return targetType.cast(boolVal);
        }
        return convertSimple(boolVal ? 1 : 0, targetType);

      case NUMERIC:
      case DECIMAL:
      case NUMERICN:
      case DECIMALN:
        return targetType.cast(readDecimal(data, scale));

      case MONEY:
      case MONEYN:
      case SMALLMONEY:
        if (targetType == BigDecimal.class) {
          if (data.length == 4) {
            int valM = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
            return targetType.cast(BigDecimal.valueOf(valM, 4));
          } else {
            ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int high = bb.getInt();
            int low = bb.getInt();
            long valM = ((long) high << 32) | (low & 0xFFFFFFFFL);
            return targetType.cast(BigDecimal.valueOf(valM, 4));
          }
        }
        throw new UnsupportedOperationException("Money conversion to " + targetType.getName()
            + " not supported");

      default:
        throw new IllegalStateException("Unexpected numeric type: " + tdsType);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T convertSimple(long val, Class<T> type) {
    if (type == Integer.class) {
      return (T) Integer.valueOf((int) val);
    }
    if (type == Long.class) {
      return (T) Long.valueOf(val);
    }
    if (type == Short.class) {
      return (T) Short.valueOf((short) val);
    }
    if (type == Byte.class) {
      return (T) Byte.valueOf((byte) val);
    }
    if (type == Boolean.class) {
      return (T) Boolean.valueOf(val != 0);
    }
    return (T) Long.valueOf(val);
  }

  private BigDecimal readDecimal(byte[] data, int scale) {
    int sign = data[0];
    byte[] mag = new byte[data.length - 1];
    for (int i = 0; i < mag.length; i++) {
      mag[i] = data[data.length - 1 - i];
    }
    BigInteger bi = new BigInteger(1, mag);
    if (sign == 0) {
      bi = bi.negate();
    }
    return new BigDecimal(bi, scale);
  }
}