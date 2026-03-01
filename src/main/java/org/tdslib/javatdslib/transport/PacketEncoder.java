package org.tdslib.javatdslib.transport;

import org.tdslib.javatdslib.packets.TdsMessage;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Abstracts the conversion of a logical TdsMessage into network-ready chunks.
 */
public interface PacketEncoder {

  /**
   * Encodes a logical TdsMessage into a list of ByteBuffers,
   * applying the TDS headers and chunking based on the maxPacketSize.
   */
  List<ByteBuffer> encodeMessage(TdsMessage message, int spid, int maxPacketSize);
}