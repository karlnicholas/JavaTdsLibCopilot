// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.messages;

import org.tdslib.javatdslib.packets.Packet;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.payloads.Payload;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * TDS Message.
 */
public class Message {
    private PacketType packetType;
    private boolean resetConnection;
    private boolean ignore;
    private Payload payload;

    public PacketType getPacketType() { return packetType; }
    public void setPacketType(PacketType packetType) { this.packetType = packetType; }

    public boolean isResetConnection() { return resetConnection; }
    public void setResetConnection(boolean resetConnection) { this.resetConnection = resetConnection; }

    public boolean isIgnore() { return ignore; }
    public void setIgnore(boolean ignore) { this.ignore = ignore; }

    public Payload getPayload() { return payload; }
    public void setPayload(Payload payload) { this.payload = payload; }

    public Message(PacketType packetType) {
        this(packetType, false);
    }

    public Message(PacketType packetType, boolean resetConnection) {
        this.packetType = packetType;
        this.resetConnection = resetConnection;
    }

    public List<Packet> getPackets(int packetSize) {
        List<Packet> packets = new ArrayList<>();
        int maxDataPerPacket = packetSize - Packet.HEADER_LENGTH;

        ByteBuffer payloadBuffer;
        if (payload == null) {
            payloadBuffer = ByteBuffer.allocate(0);
        } else {
            payloadBuffer = payload.buildBuffer();
        }

        payloadBuffer.rewind();

        int packetId = 0;
        int totalRemaining = payloadBuffer.remaining();

        if (totalRemaining == 0) {
            packets.add(createPacket(packetId, true, ByteBuffer.allocate(0)));
            return packets;
        }

        while (payloadBuffer.hasRemaining()) {
            int chunkSize = Math.min(maxDataPerPacket, payloadBuffer.remaining());

            // Create a view (slice) of the current chunk
            ByteBuffer packetData = payloadBuffer.slice().limit(chunkSize);

            // Advance original buffer position
            payloadBuffer.position(payloadBuffer.position() + chunkSize);

            boolean isLast = !payloadBuffer.hasRemaining();
            packets.add(createPacket(packetId++, isLast, packetData));
        }

        return packets;
    }

    private Packet createPacket(int id, boolean isLast, ByteBuffer data) {
        Packet p = new Packet(packetType);
        p.setPacketId(id);
        p.setLast(isLast);
        p.setIgnore(ignore);
        p.setResetConnection(resetConnection);

        if (data != null && data.hasRemaining()) {
            p.addData(data);
        }
        return p;
    }

    @Override
    public String toString() {
        return "Message=[Type=" + packetType + ", Reset=" + resetConnection + ", Ignore=" + ignore + ", Payload=" + payload + "]";
    }
}