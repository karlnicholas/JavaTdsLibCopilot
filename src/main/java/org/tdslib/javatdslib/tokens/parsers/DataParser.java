package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.streaming.TdsClob;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.nio.ByteBuffer;

public class DataParser {

  // Add Charset to the signature
  public static Object getDataBytes(ByteBuffer payload, TdsType type, int maxLength,
                                    TdsTransport transport, TdsStreamHandler decoder,
                                    java.nio.charset.Charset charset) {
    Object data = null;

    switch (type.strategy) {
      case FIXED:
        if (type == TdsType.DATE) {
          int len = payload.get() & 0xFF;
          if (len > 0) data = readBytes(payload, len);
        } else {
          data = readBytes(payload, type.fixedSize);
        }
        break;

      case SCALE_LEN:
      case PREC_SCALE:
      case BYTELEN:
        int len = payload.get() & 0xFF;
        if (len > 0) data = readBytes(payload, len);
        break;

      case USHORTLEN:
        if (maxLength == 65535) {
          data = readPlp(payload, transport, decoder, charset);
        } else {
          int varLen = Short.toUnsignedInt(payload.getShort());
          if (varLen != 0xFFFF) data = readBytes(payload, varLen);
        }
        break;

      case PLP:
        data = readPlp(payload, transport, decoder, charset);
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
    }
    return data;
  }

  private static byte[] readBytes(ByteBuffer buf, int length) {
    byte[] b = new byte[length];
    buf.get(b);
    return b;
  }

  private static Object readPlp(ByteBuffer payload, TdsTransport transport, TdsStreamHandler decoder, java.nio.charset.Charset charset) {
    long totalLength = payload.getLong();
    if (totalLength == -1L && payload.remaining() == 0) return null;
    if (totalLength == 0xFFFFFFFFFFFFFFFFL) return null;

    transport.suspendNetworkRead();

    ByteBuffer leftover = null;
    if (payload.hasRemaining()) {
      leftover = ByteBuffer.allocate(payload.remaining()).order(java.nio.ByteOrder.LITTLE_ENDIAN);
      leftover.put(payload);
      leftover.flip();
    }
    // Pass charset to the proxy
    return new org.tdslib.javatdslib.streaming.TdsClob(transport, decoder, leftover, charset);
  }
}