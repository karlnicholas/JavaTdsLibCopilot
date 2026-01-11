package org.tdslib.javatdslib.messages;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes TDS packets. Handles splitting large payloads into multiple packets.
 */
public class TdsPacketWriter {

  /**
   * Builds one or more TDS packets from a payload.
   *
   * @param packetType       TDS message type (e.g. 0x01 for SQL Batch, 0x10 for Login7)
   * @param statusFlags      status flags (usually 0x01 for last packet/EOM)
   * @param payload          the logical message payload (positioned at 0)
   * @param startingPacketId starting packet number (usually 1 for client requests)
   * @param maxPacketSize    maximum allowed packet size (default 4096)
   * @return list of ready-to-send ByteBuffers (each is a full 8-byte header + payload chunk)
   */
  public List<ByteBuffer> buildPackets(
      byte packetType,
      byte statusFlags,
      int spid,
      ByteBuffer payload,
      short startingPacketId,
      int maxPacketSize) {

    List<ByteBuffer> packets = new ArrayList<>();
    short packetId = startingPacketId;

    int maxPayloadPerPacket = maxPacketSize - 8;

    payload = payload.asReadOnlyBuffer();
    payload.rewind();

    boolean isFirst = true;

    while (payload.hasRemaining() || isFirst) {
      isFirst = false;

      int thisPayloadSize = Math.min(maxPayloadPerPacket, payload.remaining());

      // For multi-packet: only last packet has EOM (0x01)
      // For single-packet: always set EOM
      boolean isLast = !payload.hasRemaining() || thisPayloadSize == payload.remaining();
      byte thisStatus = (byte) (isLast ? (statusFlags | 0x01) : (statusFlags & ~0x01));

      ByteBuffer packet = ByteBuffer.allocate(8 + thisPayloadSize)
          .order(ByteOrder.BIG_ENDIAN);

      packet.put(packetType);                    // Byte 0: Type
      packet.put(thisStatus);                    // Byte 1: Status (EOM on last)
      packet.putShort((short) (8 + thisPayloadSize)); // Bytes 2-3: Length (BE)
      packet.putShort((short) spid);                // Bytes 4-5: SPID (0 for client)
      packet.put((byte) (packetId & 0xFF));      // Byte 6: Packet Number (1 byte)
      packet.put((byte) 0);                      // Byte 7: Window (always 0)

      if (thisPayloadSize > 0) {
        ByteBuffer chunk = payload.slice().limit(thisPayloadSize);
        packet.put(chunk);
        payload.position(payload.position() + thisPayloadSize);
      }

      packet.flip();
      packets.add(packet);

      packetId++;
    }

    return packets;
  }
}