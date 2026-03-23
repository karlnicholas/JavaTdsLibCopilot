package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.protocol.TdsType;

import java.nio.ByteBuffer;

/**
 * Parser for reading data values from a TDS stream based on the data type. This class handles the
 * extraction of raw bytes or streaming objects (BLOB/CLOB) from the payload.
 */
public class DataParser {

  /**
   * Reads data bytes from the payload based on the TDS type and length strategy.
   *
   * @param payload The buffer containing the data.
   * @param type The TDS data type.
   * @param maxLength The maximum length of the data.
   * @param charset The charset to use for character data.
   * @return The read data as an Object (byte[] or TdsBlob/TdsClob).
   */
  public static Object getDataBytes(
      ByteBuffer payload,
      TdsType type,
      int maxLength,
      java.nio.charset.Charset charset) {
    Object data = null;

    switch (type.strategy) {
      case FIXED:
        if (type == TdsType.DATE) {
          int len = payload.get() & 0xFF;
          if (len > 0) {
            data = readBytes(payload, len);
          }
        } else {
          data = readBytes(payload, type.fixedSize);
        }
        break;

      case SCALE_LEN:
      case PREC_SCALE:
      case BYTELEN:
        int len = payload.get() & 0xFF;
        if (len > 0) {
          data = readBytes(payload, len);
        }
        break;

      case USHORTLEN:
        if (maxLength == 65535) {
          // Pass the type parameter down
          data = readPlp(payload, charset, type);
        } else {
          int varLen = Short.toUnsignedInt(payload.getShort());
          if (varLen != 0xFFFF) {
            data = readBytes(payload, varLen);
          }
        }
        break;

      case PLP:
        // Pass the type parameter down
        data = readPlp(payload, charset, type);
        break;

      case LONGLEN:
        int textPtrLen = payload.get() & 0xFF;
        if (textPtrLen == 0) {
          data = null;
        } else {
          byte[] txtPtr = new byte[textPtrLen];
          payload.get(txtPtr);
          byte[] timestamp = new byte[8];
          payload.get(timestamp);
          int dataLen = payload.getInt();
          if (dataLen > 0) {
            // FIX: Read into a typed byte array first, then assign to the Object (fixes Error 3)
            byte[] tempBytes = new byte[dataLen];
            payload.get(tempBytes);
            data = tempBytes;
          } else {
            data = new byte[0];
          }
        }
        break;

      default:
        throw new IllegalArgumentException("Unsupported length strategy: " + type.strategy);
    }
    return data;
  }

  private static byte[] readBytes(ByteBuffer buf, int length) {
    byte[] b = new byte[length];
    buf.get(b);
    return b;
  }

  private static Object readPlp(
      ByteBuffer payload,
      java.nio.charset.Charset charset,
      TdsType type) {

    long totalLength = payload.getLong();
    if (totalLength == -1L && payload.remaining() == 0) return null;
    if (totalLength == 0xFFFFFFFFFFFFFFFFL) return null;

    // 1. Capture ONLY the leftover bytes in the CURRENT network packet.
    // Do NOT loop and steal the whole stream!
    ByteBuffer leftover = null;
    if (payload.hasRemaining()) {
      leftover = ByteBuffer.allocate(payload.remaining()).order(java.nio.ByteOrder.LITTLE_ENDIAN);
      leftover.put(payload);
      leftover.flip();

      // 2. Consume the payload buffer so the Row Parser knows we took these bytes
      payload.position(payload.limit());
    }

    // 4. Return the streaming objects tied to the network
    if (type == TdsType.BIGVARBIN || type == TdsType.BIGBINARY || type == TdsType.IMAGE) {
      return new org.tdslib.javatdslib.streaming.TdsBlob(leftover);
    } else {
      return new org.tdslib.javatdslib.streaming.TdsClob(leftover, charset);
    }
  }
  /**
   * Safely calculates the number of bytes required to read the data value from the stream,
   * advancing the peekBuffer position if enough bytes are available.
   *
   * @param peekBuffer A read-only duplicate of the current network buffer.
   * @param type       The TDS data type.
   * @param maxLength  The maximum length of the data (from TypeInfo).
   * @return The number of bytes required, or -1 if the buffer underflows.
   */
  public static int getRequiredValueBytes(ByteBuffer peekBuffer, TdsType type, int maxLength) {
    int startPos = peekBuffer.position();

    switch (type.strategy) {
      case FIXED:
        if (type == TdsType.DATE) {
          if (peekBuffer.remaining() < 1) return -1;
          int len = peekBuffer.get() & 0xFF;
          if (len > 0) {
            if (peekBuffer.remaining() < len) return -1;
            peekBuffer.position(peekBuffer.position() + len);
          }
        } else {
          if (peekBuffer.remaining() < type.fixedSize) return -1;
          peekBuffer.position(peekBuffer.position() + type.fixedSize);
        }
        break;

      case SCALE_LEN:
      case PREC_SCALE:
      case BYTELEN:
        if (peekBuffer.remaining() < 1) return -1;
        int len = peekBuffer.get() & 0xFF;
        if (len > 0) {
          if (peekBuffer.remaining() < len) return -1;
          peekBuffer.position(peekBuffer.position() + len);
        }
        break;

      case USHORTLEN:
        if (maxLength == 65535) { // PLP stream
          if (!checkPlpBytes(peekBuffer)) return -1;
        } else {
          if (peekBuffer.remaining() < 2) return -1;
          int varLen = Short.toUnsignedInt(peekBuffer.getShort());
          if (varLen != 0xFFFF && varLen > 0) { // 0xFFFF means NULL
            if (peekBuffer.remaining() < varLen) return -1;
            peekBuffer.position(peekBuffer.position() + varLen);
          }
        }
        break;

      case PLP:
        if (!checkPlpBytes(peekBuffer)) return -1;
        break;

      case LONGLEN:
        if (peekBuffer.remaining() < 1) return -1;
        int textPtrLen = peekBuffer.get() & 0xFF;
        if (textPtrLen > 0) {
          // Need textPtrLen + 8 (timestamp) + 4 (dataLen) = 12 bytes of fixed headers
          if (peekBuffer.remaining() < (textPtrLen + 12)) return -1;

          peekBuffer.position(peekBuffer.position() + textPtrLen + 8); // Skip text pointer and timestamp
          int dataLen = peekBuffer.getInt();

          if (dataLen > 0) {
            if (peekBuffer.remaining() < dataLen) return -1;
            peekBuffer.position(peekBuffer.position() + dataLen);
          }
        }
        break;

      default:
        throw new IllegalArgumentException("Unsupported length strategy: " + type.strategy);
    }

    return peekBuffer.position() - startPos;
  }

  /**
   * Safely checks the bounds for a PLP (Partially Length-Prefixed) stream.
   * Matches the current getDataBytes behavior of consuming the rest of the available packet.
   */
  private static boolean checkPlpBytes(ByteBuffer peekBuffer) {
    if (peekBuffer.remaining() < 8) return false;

    long totalLength = peekBuffer.getLong();

    // -1L (0xFFFFFFFFFFFFFFFFL) represents a NULL PLP value.
    // If it's not null, we mirror the current stream-stealing behavior.
    if (totalLength != -1L) {
      peekBuffer.position(peekBuffer.limit());
    }

    return true;
  }
}
