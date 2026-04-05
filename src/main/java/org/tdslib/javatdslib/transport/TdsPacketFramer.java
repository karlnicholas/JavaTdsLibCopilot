package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Extracts complete TDS packets from the network stream, strips the 8-byte headers,
 * and forwards the full payloads to the TdsStreamHandler.
 */
public class TdsPacketFramer {
  private static final Logger logger = LoggerFactory.getLogger(TdsPacketFramer.class);
  private static final int TDS_HEADER_LENGTH = 8;
  private static final int STATUS_OFFSET = 1;
  private static final int EOM_BIT = 0x01;

  private final TdsStreamHandler streamHandler;

  public TdsPacketFramer(TdsStreamHandler streamHandler) {
    this.streamHandler = streamHandler;
  }

  public void decode(ByteBuffer networkBuffer) {
    logger.trace(">>> [Framer] ENTER: Analyzing {} bytes", networkBuffer.remaining());

    while (networkBuffer.hasRemaining()) {

      // 1. Check if we have enough bytes for the header
      if (networkBuffer.remaining() < TDS_HEADER_LENGTH) {
        return;
      }

      int headerStart = networkBuffer.position();
      int packetLength = Short.toUnsignedInt(networkBuffer.getShort(headerStart + 2));

      // Prevent Infinite NIO Spin on corrupted headers
      if (packetLength > networkBuffer.capacity()) {
        throw new IllegalStateException("Protocol Desync: Packet length (" + packetLength +
            " bytes) exceeds physical buffer capacity (" + networkBuffer.capacity() + " bytes).");
      }

      // 2. Check if the ENTIRE packet has arrived from the network
      if (networkBuffer.remaining() < packetLength) {
        logger.trace(">>> [Framer] WAIT: Need {} bytes for full packet, but only have {}", packetLength, networkBuffer.remaining());
        return;
      }

      // 2. Check if the ENTIRE packet has arrived from the network
      if (networkBuffer.remaining() < packetLength) {
        logger.trace(">>> [Framer] WAIT: Need {} bytes for full packet, but only have {}", packetLength, networkBuffer.remaining());
        return;
      }

      // 3. We have a complete packet. Parse header metadata.
      byte status = networkBuffer.get(headerStart + STATUS_OFFSET);
      boolean isEom = (status & EOM_BIT) != 0;
      int payloadLength = packetLength - TDS_HEADER_LENGTH;

      // Skip the 8-byte header
      networkBuffer.position(headerStart + TDS_HEADER_LENGTH);

      // 4. Create a strict slice for the payload
      int originalLimit = networkBuffer.limit();
      networkBuffer.limit(networkBuffer.position() + payloadLength);
      ByteBuffer payloadSlice = networkBuffer.slice();

      logger.trace(">>> [Framer] HANDOFF: Sliced {} byte payload. (isEom: {})", payloadLength, isEom);

      // 5. Fire and forget. We assume the handler fully processes this discrete frame.
      if (isEom) {
        logger.debug(">>> [Framer] EOM: Reached End of Message for the current TDS response.");
      }
      streamHandler.onPayloadAvailable(payloadSlice, isEom);

      // 6. Advance the main buffer past the payload we just delivered
      networkBuffer.limit(originalLimit);
      networkBuffer.position(networkBuffer.position() + payloadLength);
    }

    logger.trace(">>> [Framer] EXIT: networkBuffer fully processed.");
  }
}