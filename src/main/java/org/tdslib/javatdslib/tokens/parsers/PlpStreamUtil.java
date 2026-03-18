package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.nio.ByteBuffer;

public class PlpStreamUtil {

  public static Object readPlp(
      ByteBuffer payload,
      TdsTransport transport,
      TdsStreamHandler decoder,
      java.nio.charset.Charset charset,
      TdsType type) {

    long totalLength = payload.getLong();
    if (totalLength == -1L && payload.remaining() == 0) return null;
    if (totalLength == 0xFFFFFFFFFFFFFFFFL) return null;

    ByteBuffer leftover = null;
    if (payload.hasRemaining()) {
      leftover = ByteBuffer.allocate(payload.remaining()).order(java.nio.ByteOrder.LITTLE_ENDIAN);
      leftover.put(payload);
      leftover.flip();
      payload.position(payload.limit());
    }

    transport.suspendNetworkRead();

    if (type == TdsType.BIGVARBIN || type == TdsType.BIGBINARY || type == TdsType.IMAGE) {
      return new org.tdslib.javatdslib.streaming.TdsBlob(leftover);
    } else {
      return new org.tdslib.javatdslib.streaming.TdsClob(leftover, charset);
    }
  }
}