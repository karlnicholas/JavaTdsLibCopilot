package org.tdslib.javatdslib.codec;

import org.tdslib.javatdslib.protocol.TdsType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

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
        return convertSimple(data[0] & 0xFF, 1, targetType);
      case INT2:
        return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort(), 2, targetType);
      case INT4:
        return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt(), 4, targetType);
      case INT8:
        return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong(), 8, targetType);
      case INTN:
        if (data.length == 1) {
          return convertSimple(data[0] & 0xFF, 1, targetType);
        }
        if (data.length == 2) {
          return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getShort(), 2, targetType);
        }
        if (data.length == 4) {
          return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt(), 4, targetType);
        }
        if (data.length == 8) {
          return convertSimple(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong(), 8, targetType);
        }
        throw new IllegalStateException("Unexpected INTN length");

      case FLT4:
        float flt4Val = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getFloat();
        if (targetType == Double.class) {
          return targetType.cast((double) flt4Val); // Widen safely
        }
        return targetType.cast(flt4Val);

      case FLTN:
        if (data.length == 4) {
          float fltnFloat = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getFloat();
          if (targetType == Double.class) {
            return targetType.cast((double) fltnFloat); // Widen safely
          }
          return targetType.cast(fltnFloat);
        }
        double fltnDouble = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getDouble();
        if (targetType == Float.class) {
          return targetType.cast((float) fltnDouble);
        }
        return targetType.cast(fltnDouble); // Defaults to Double for Object.class

      case FLT8:
        double flt8Double = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getDouble();
        if (targetType == Float.class) {
          return targetType.cast((float) flt8Double);
        }
        return targetType.cast(flt8Double);

      case BIT:
      case BITN:
        boolean boolVal = data[0] != 0;
        // FIX: Allow Object.class to naturally yield a Boolean
        if (targetType == Boolean.class || targetType == Object.class) {
          return targetType.cast(boolVal);
        }
        return convertSimple(boolVal ? 1 : 0, 1, targetType);

      case NUMERIC:
      case DECIMAL:
      case NUMERICN:
      case DECIMALN:
        return targetType.cast(readDecimal(data, scale));

      case MONEY:
      case MONEYN:
      case SMALLMONEY:
        // FIX: Allow Object.class to naturally yield a BigDecimal
        if (targetType == BigDecimal.class || targetType == Object.class) {
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
  private <T> T convertSimple(long val, int byteLength, Class<T> type) {
    if (type == Integer.class || (type == Object.class && byteLength == 4)) {
      return (T) Integer.valueOf((int) val);
    }
    if (type == Long.class || (type == Object.class && byteLength == 8)) {
      return (T) Long.valueOf(val);
    }
    // 1. Explicit Byte Request: Honor the contract, let the client handle overflow
    if (type == Byte.class) {
      return type.cast((byte) val);
    }
    // 2. Explicit Short Request OR Generic Object Request (Safe Default for TINYINT/SMALLINT)
    if (type == Short.class || (type == Object.class && byteLength <= 2)) {
      return (T) Short.valueOf((short) val);
    }
    if (type == Boolean.class) {
      return (T) Boolean.valueOf(val != 0);
    }

    // Safest ultimate fallback
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