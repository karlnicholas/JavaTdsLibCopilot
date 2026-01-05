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
     * @param packetType     TDS message type (e.g. 0x01 for SQL Batch)
     * @param statusFlags    status flags (usually 0x01 for last packet)
     * @param payload        the logical message payload
     * @param startingPacketId starting packet number
     * @param maxPacketSize  maximum allowed packet size (from ENVCHANGE or default 4096)
     * @return list of ready-to-send ByteBuffers (each is a full packet)
     */
    public List<ByteBuffer> buildPackets(
            byte packetType,
            byte statusFlags,
            ByteBuffer payload,
            short startingPacketId,
            int maxPacketSize) {

        List<ByteBuffer> packets = new ArrayList<>();
        short packetId = startingPacketId;

        // Max payload per packet = maxPacketSize - 8 (header)
        int maxPayloadPerPacket = maxPacketSize - 8;

        payload = payload.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
        payload.rewind();

        while (payload.hasRemaining()) {
            int thisPayloadSize = Math.min(maxPayloadPerPacket, payload.remaining());

            // For all but the last packet, clear EOM flag
            byte thisStatus = (byte) (packetType == payload.remaining() ? statusFlags : statusFlags & ~0x01);

            ByteBuffer packet = ByteBuffer.allocate(8 + thisPayloadSize)
                    .order(ByteOrder.BIG_ENDIAN);

            packet.put(packetType);
            packet.put(thisStatus);
            packet.putShort((short) (8 + thisPayloadSize)); // length
            packet.putShort((short) 0); // SPID (usually 0 for client)
            packet.putShort(packetId++);
            packet.put((byte) 0); // window

            // Copy payload chunk
            ByteBuffer chunk = payload.slice().limit(thisPayloadSize);
            packet.put(chunk);
            payload.position(payload.position() + thisPayloadSize);

            packet.flip();
            packets.add(packet);
        }

        // Ensure last packet has EOM flag set
        if (!packets.isEmpty()) {
            ByteBuffer last = packets.get(packets.size() - 1);
            last.put(1, (byte) (last.get(1) | 0x01)); // force EOM
        }

        return packets;
    }
}