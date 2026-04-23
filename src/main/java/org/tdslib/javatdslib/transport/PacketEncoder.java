package org.tdslib.javatdslib.transport;

import org.tdslib.javatdslib.packets.OutboundTdsMessage;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Abstracts the conversion of a logical OutboundTdsMessage into network-ready chunks.
 */
public interface PacketEncoder {

  /**
   * Encodes a logical OutboundTdsMessage into a list of ByteBuffers,
   * applying the TDS headers and chunking based on the maxPacketSize.
   */
  List<ByteBuffer> encodeMessage(OutboundTdsMessage message, int spid, int maxPacketSize);
}