// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.packets;

import java.nio.ByteBuffer;
import com.microsoft.data.tools.tdslib.buffer.ByteBufferUtil;

/**
 * Represents a Packet in the TDS protocol.
 */
public class Packet {
    // Constants

    /**
     * Header length.
     */
    public static final byte HEADER_LENGTH = 8;

    /**
     * Default SPID value.
     */
    public static final short DEFAULT_SPID = 0;

    /**
     * Default Packet Id.
     */
    public static final byte DEFAULT_PACKET_ID = 1;

    /**
     * Default Window.
     */
    public static final byte DEFAULT_WINDOW = 0;

    /**
     * Buffer containing the packet (header + data).
     */
    private ByteBuffer buffer;

    /**
     * Type of the packet.
     */
    public PacketType getType() {
        buffer.position(PacketOffset.TYPE);
        return PacketType.values()[ByteBufferUtil.readUInt8(buffer)];
    }

    /**
     * Status of the packet.
     */
    public byte getStatus() {
        buffer.position(PacketOffset.STATUS);
        return (byte) ByteBufferUtil.readUInt8(buffer);
    }

    /**
     * Server Process Id.
     */
    public int getSPId() {
        buffer.position(PacketOffset.SPID);
        return ByteBufferUtil.readUInt16BE(buffer);
    }

    /**
     * Packet Id.
     */
    public byte getId() {
        buffer.position(PacketOffset.PACKET_ID);
        return (byte) ByteBufferUtil.readUInt8(buffer);
    }

    public void setId(byte id) {
        buffer.position(PacketOffset.PACKET_ID);
        ByteBufferUtil.writeUInt8(buffer, id);
    }

    /**
     * Window value.
     */
    public byte getWindow() {
        buffer.position(PacketOffset.WINDOW);
        return (byte) ByteBufferUtil.readUInt8(buffer);
    }

    /**
     * Total length of the packet (header + data).
     */
    public int getLength() {
        buffer.position(PacketOffset.LENGTH);
        return ByteBufferUtil.readUInt16BE(buffer);
    }

    /**
     * Indicates if this packet is the last packet for a Message.
     */
    public boolean isLast() {
        return (getStatus() & PacketStatus.EOM) == PacketStatus.EOM;
    }

    public void setLast(boolean last) {
        buffer.position(PacketOffset.STATUS);
        byte status = (byte) ByteBufferUtil.readUInt8(buffer);
        if (last) {
            status |= PacketStatus.EOM;
        } else {
            status &= (byte) ~PacketStatus.EOM;
        }
        buffer.position(PacketOffset.STATUS);
        ByteBufferUtil.writeUInt8(buffer, status);
    }

    /**
     * Indicates if the packet (and message) should be ignored.
     */
    public boolean isIgnore() {
        return (getStatus() & PacketStatus.IGNORE) == PacketStatus.IGNORE;
    }

    public void setIgnore(boolean ignore) {
        buffer.position(PacketOffset.STATUS);
        byte status = (byte) ByteBufferUtil.readUInt8(buffer);
        if (ignore) {
            status |= PacketStatus.IGNORE;
        } else {
            status &= (byte) ~PacketStatus.IGNORE;
        }
        buffer.position(PacketOffset.STATUS);
        ByteBufferUtil.writeUInt8(buffer, status);
    }

    /**
     * Indicates if the connection should be reset.
     */
    public boolean isResetConnection() {
        return (getStatus() & PacketStatus.RESET_CONNECTION) == PacketStatus.RESET_CONNECTION;
    }

    public void setResetConnection(boolean resetConnection) {
        buffer.position(PacketOffset.STATUS);
        byte status = (byte) ByteBufferUtil.readUInt8(buffer);
        if (resetConnection) {
            status |= PacketStatus.RESET_CONNECTION;
        } else {
            status &= (byte) ~PacketStatus.RESET_CONNECTION;
        }
        buffer.position(PacketOffset.STATUS);
        ByteBufferUtil.writeUInt8(buffer, status);
    }

    /**
     * Gets a copy of the data of this packet.
     * May be empty if there is no data in the packet.
     */
    public ByteBuffer getData() {
        if (buffer.capacity() == HEADER_LENGTH) {
            return ByteBufferUtil.allocate(0);
        }
        buffer.position(HEADER_LENGTH);
        ByteBuffer data = buffer.slice();
        data.order(buffer.order());
        return data;
    }

    /**
     * Creates a new packet with the specified type.
     */
    public Packet(PacketType type) {
        buffer = ByteBufferUtil.allocate(HEADER_LENGTH);

        buffer.position(PacketOffset.TYPE);
        ByteBufferUtil.writeUInt8(buffer, type.getValue());
        buffer.position(PacketOffset.STATUS);
        ByteBufferUtil.writeUInt8(buffer, PacketStatus.NORMAL);
        buffer.position(PacketOffset.SPID);
        ByteBufferUtil.writeUInt16BE(buffer, DEFAULT_SPID);
        buffer.position(PacketOffset.PACKET_ID);
        ByteBufferUtil.writeUInt8(buffer, DEFAULT_PACKET_ID);
        buffer.position(PacketOffset.WINDOW);
        ByteBufferUtil.writeUInt8(buffer, DEFAULT_WINDOW);
        updateLength();
    }

    /**
     * Creates a packet from a buffer.
     */
    public Packet(ByteBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }
        if (buffer.capacity() < HEADER_LENGTH) {
            throw new IllegalArgumentException("Buffer length must be greater than the packet header length");
        }
        buffer.position(PacketOffset.TYPE);
        byte typeValue = (byte) ByteBufferUtil.readUInt8(buffer);
        if (typeValue > PacketType.PRE_LOGIN.getValue()) {
            throw new IllegalArgumentException("Invalid packet type: 0x" + String.format("%02X", typeValue));
        }
        this.buffer = buffer;
    }

    private void updateLength() {
        buffer.position(PacketOffset.LENGTH);
        ByteBufferUtil.writeUInt16BE(buffer, buffer.capacity());
    }

    /**
     * Sets the Packet Id of this packet with an integer. The value will be truncated to byte.
     */
    public void setPacketId(int packetId) {
        setId((byte) (packetId % 256));
    }

    /**
     * Adds data to this packet.
     */
    public void addData(ByteBuffer data) {
        ByteBuffer newBuffer = ByteBufferUtil.allocate(buffer.capacity() + data.capacity());
        buffer.rewind();
        newBuffer.put(buffer);
        newBuffer.put(data);
        this.buffer = newBuffer;
        updateLength();
    }

    /**
     * Returns a human readable string representation of this object.
     */
    @Override
    public String toString() {
        return String.format("Packet[Type=0x%02X(%s), Status=0x%02X, Length=%d, SPID=0x%04X, PacketId=%d, Window=0x%02X]",
                getType().getValue(), getType(), getStatus(), getLength(), getSPId(), getId(), getWindow());
    }
}