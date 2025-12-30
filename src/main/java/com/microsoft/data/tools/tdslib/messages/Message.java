// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.messages;

import com.microsoft.data.tools.tdslib.packets.Packet;
import com.microsoft.data.tools.tdslib.packets.PacketType;
import com.microsoft.data.tools.tdslib.payloads.Payload;
import com.microsoft.data.tools.tdslib.buffer.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import java.util.List;

/**
 * TDS Message.
 */
public class Message {
    private PacketType packetType;
    private boolean resetConnection;
    private boolean ignore;
    private Payload payload;

    /**
     * The packet type of the packets of this message.
     */
    public PacketType getPacketType() {
        return packetType;
    }

    public void setPacketType(PacketType packetType) {
        this.packetType = packetType;
    }

    /**
     * Indicates if the connection should be reset.
     */
    public boolean isResetConnection() {
        return resetConnection;
    }

    public void setResetConnection(boolean resetConnection) {
        this.resetConnection = resetConnection;
    }

    /**
     * Indicates if the message should be ignored.
     */
    public boolean isIgnore() {
        return ignore;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    /**
     * The payload of the message.
     */
    public Payload getPayload() {
        return payload;
    }

    public void setPayload(Payload payload) {
        this.payload = payload;
    }

    /**
     * Creates a new empty message.
     */
    public Message(PacketType packetType) {
        this(packetType, false);
    }

    public Message(PacketType packetType, boolean resetConnection) {
        this.packetType = packetType;
        this.resetConnection = resetConnection;
    }

    /**
     * Get the packets of this message.
     */
    public List<Packet> getPackets(int packetSize) {
        List<Packet> packets = new ArrayList<>();

        int maxDataPerPacket = packetSize - Packet.HEADER_LENGTH;

        ByteBuffer payloadBuffer = (payload == null) ? ByteBufferUtil.EMPTY : payload.buildBuffer();
        payloadBuffer.rewind();

        int packetId = 1;

        if (payloadBuffer.remaining() == 0) {
            Packet p = new Packet(packetType);
            p.setPacketId(packetId);
            p.setLast(true);
            if (ignore) p.setIgnore(true);
            if (resetConnection) p.setResetConnection(true);
            packets.add(p);
            return packets;
        }

        while (payloadBuffer.remaining() > 0) {
            int chunk = Math.min(maxDataPerPacket, payloadBuffer.remaining());
            ByteBuffer data = ByteBufferUtil.allocate(chunk);
            byte[] tmp = new byte[chunk];
            payloadBuffer.get(tmp);
            data.put(tmp);
            data.rewind();

            Packet p = new Packet(packetType);
            p.setPacketId(packetId++);
            p.addData(data);
            if (payloadBuffer.remaining() == 0) {
                p.setLast(true);
            }
            if (ignore) p.setIgnore(true);
            if (resetConnection) p.setResetConnection(true);
            packets.add(p);
        }

        return packets;
    }

    @Override
    public String toString() {
        return "Message=[Type=" + packetType + ", Reset=" + resetConnection + ", Ignore=" + ignore + ", Payload=" + payload + "]";
    }
}