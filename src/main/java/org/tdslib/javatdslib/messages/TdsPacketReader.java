package org.tdslib.javatdslib.messages;

import org.tdslib.javatdslib.transport.TcpTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Reads a complete TDS packet from the transport.
 */
public class TdsPacketReader {

    private final TcpTransport transport;

    public TdsPacketReader(TcpTransport transport) {
        this.transport = transport;
    }

    /**
     * Reads exactly one complete TDS packet.
     * Returns the full packet as a ByteBuffer (header + payload).
     */
    public ByteBuffer readRawPacket() throws IOException {
        // First, read the fixed 8-byte header
        ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        transport.readFully(header);
        header.flip();

        byte packetType = header.get();
        byte status = header.get();
        int length = header.getShort() & 0xFFFF;  // unsigned short

        if (length < 8 || length > 32767) {
            throw new IOException("Invalid TDS packet length: " + length);
        }

        // Now read the remaining payload
        ByteBuffer fullPacket = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
        fullPacket.put(header.array());  // copy header back in
        ByteBuffer payloadPart = fullPacket.slice();
        payloadPart.position(8);

        transport.readFully(payloadPart);

        fullPacket.flip();
        return fullPacket;
    }

    /**
     * Convenience method: read a full packet and return it as a Message.
     */
    public Message readPacket() throws IOException {
        ByteBuffer raw = readRawPacket();
        raw.mark();

        byte type = raw.get();
        byte status = raw.get();
        int length = Short.toUnsignedInt(raw.getShort());
        short spid = raw.getShort();
        short packetId = raw.getShort();
        raw.get(); // window

        raw.reset();
        raw.position(8);
        ByteBuffer payload = raw.slice().limit(length - 8);

        return new Message(
                type,
                status,
                length,
                spid,
                packetId,
                payload,
                System.nanoTime(),
                null
        );
    }
}