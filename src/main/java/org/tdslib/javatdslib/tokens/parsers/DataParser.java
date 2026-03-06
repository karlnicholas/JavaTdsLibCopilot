package org.tdslib.javatdslib.tokens.parsers;

import java.nio.ByteBuffer;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

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
   * @param transport The transport layer for reading streaming data.
   * @param decoder The stream handler for processing streaming data.
   * @param charset The charset to use for character data.
   * @return The read data as an Object (byte[] or TdsBlob/TdsClob).
   */
  public static Object getDataBytes(
      ByteBuffer payload,
      TdsType type,
      int maxLength,
      TdsTransport transport,
      TdsStreamHandler decoder,
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
          data = readPlp(payload, transport, decoder, charset, type);
        } else {
          int varLen = Short.toUnsignedInt(payload.getShort());
          if (varLen != 0xFFFF) {
            data = readBytes(payload, varLen);
          }
        }
        break;

      case PLP:
        // Pass the type parameter down
        data = readPlp(payload, transport, decoder, charset, type);
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
      TdsTransport transport,
      TdsStreamHandler decoder,
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

    // 3. Suspend network reads! The StreamHandler will open the valve when the user subscribes.
    transport.suspendNetworkRead();

    // 4. Return the streaming objects tied to the network
    if (type == TdsType.BIGVARBIN || type == TdsType.BIGBINARY || type == TdsType.IMAGE) {
      return new org.tdslib.javatdslib.streaming.TdsBlob(transport, decoder, leftover);
    } else {
      return new org.tdslib.javatdslib.streaming.TdsClob(transport, decoder, leftover, charset);
    }
  }
}
