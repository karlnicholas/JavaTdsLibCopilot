// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.packets;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Represents a Packet in the TDS protocol.
 */
public class Packet {

    // Constants
    public static final int HEADER_LENGTH = 8;
    public static final int DEFAULT_SPID = 0;
    public static final int DEFAULT_PACKET_ID = 0;
    public static final int DEFAULT_WINDOW = 0;

    // Offsets for absolute access
    private static final int OFFSET_TYPE = 0;
    private static final int OFFSET_STATUS = 1;
    private static final int OFFSET_LENGTH = 2;
    private static final int OFFSET_SPID = 4;
    private static final int OFFSET_PACKET_ID = 6;
    private static final int OFFSET_WINDOW = 7;

    private ByteBuffer buffer;

    // --- Absolute Accessors ---

    public PacketType getType() {
        int typeVal = Byte.toUnsignedInt(buffer.get(OFFSET_TYPE));
        try {
            return PacketType.valueOf(typeVal);
        } catch (IllegalArgumentException e) {
            throw e;
        }
    }

    public int getStatus() {
        return Byte.toUnsignedInt(buffer.get(OFFSET_STATUS));
    }

    public int getSPId() {
        return Short.toUnsignedInt(buffer.getShort(OFFSET_SPID));
    }

    public int getId() {
        return Byte.toUnsignedInt(buffer.get(OFFSET_PACKET_ID));
    }

    public void setId(int id) {
        buffer.put(OFFSET_PACKET_ID, (byte) id);
    }

    public int getWindow() {
        return Byte.toUnsignedInt(buffer.get(OFFSET_WINDOW));
    }

    public int getLength() {
        return Short.toUnsignedInt(buffer.getShort(OFFSET_LENGTH));
    }

    // --- Status Flags ---

    public boolean isLast() {
        return (getStatus() & PacketStatus.EOM) == PacketStatus.EOM;
    }

    public void setLast(boolean last) {
        int status = getStatus();
        if (last) {
            status |= PacketStatus.EOM;
        } else {
            status &= ~PacketStatus.EOM;
        }
        buffer.put(OFFSET_STATUS, (byte) status);
    }

    public boolean isIgnore() {
        return (getStatus() & PacketStatus.IGNORE) == PacketStatus.IGNORE;
    }

    public void setIgnore(boolean ignore) {
        int status = getStatus();
        if (ignore) {
            status |= PacketStatus.IGNORE;
        } else {
            status &= ~PacketStatus.IGNORE;
        }
        buffer.put(OFFSET_STATUS, (byte) status);
    }

    public boolean isResetConnection() {
        return (getStatus() & PacketStatus.RESET_CONNECTION) == PacketStatus.RESET_CONNECTION;
    }

    public void setResetConnection(boolean resetConnection) {
        int status = getStatus();
        if (resetConnection) {
            status |= PacketStatus.RESET_CONNECTION;
        } else {
            status &= ~PacketStatus.RESET_CONNECTION;
        }
        buffer.put(OFFSET_STATUS, (byte) status);
    }

    /**
     * Returns a slice of the data portion.
     * CRITICAL: Forces Little Endian order for the payload.
     */
    public ByteBuffer getData() {
        if (buffer.capacity() <= HEADER_LENGTH) {
            return ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);
        }
        // slice() resets to Big Endian. We must force Little Endian for TDS payloads.
        return buffer.slice(HEADER_LENGTH, buffer.capacity() - HEADER_LENGTH)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asReadOnlyBuffer();
    }

    /**
     * Returns the full backing buffer (Header + Data).
     * Used when writing the packet to the network.
     */
    public ByteBuffer getBuffer() {
        // Return a duplicate so the caller cannot mess up the position/limit
        // of the internal buffer logic.
        return buffer.duplicate().rewind();
    }

    // --- Constructors ---

    public Packet(PacketType type) {
        // Headers are always Big Endian
        this.buffer = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN);

        buffer.put(OFFSET_TYPE, (byte) type.getValue());
        buffer.put(OFFSET_STATUS, PacketStatus.NORMAL);
        buffer.putShort(OFFSET_SPID, (short) DEFAULT_SPID);
        buffer.put(OFFSET_PACKET_ID, (byte) DEFAULT_PACKET_ID);
        buffer.put(OFFSET_WINDOW, (byte) DEFAULT_WINDOW);
        updateLength();
    }

    public Packet(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "Buffer cannot be null");

        // Ensure we treat headers as Big Endian
        if (buffer.order() != ByteOrder.BIG_ENDIAN) {
            buffer.order(ByteOrder.BIG_ENDIAN);
        }

        if (buffer.capacity() < HEADER_LENGTH) {
            throw new IllegalArgumentException("Buffer length must be at least " + HEADER_LENGTH);
        }

        int typeValue = Byte.toUnsignedInt(buffer.get(OFFSET_TYPE));
        try {
            PacketType.valueOf(typeValue);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Invalid packet type: 0x%02X", typeValue), e);
        }

        this.buffer = buffer;
    }

    private void updateLength() {
        buffer.putShort(OFFSET_LENGTH, (short) buffer.capacity());
    }

    public void setPacketId(int packetId) {
        setId(packetId % 256);
    }

    public void addData(ByteBuffer data) {
        if (data == null || !data.hasRemaining()) {
            return;
        }

        int newCapacity = this.buffer.capacity() + data.remaining();
        ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity).order(ByteOrder.BIG_ENDIAN);

        // Copy existing packet content without moving source position
        ByteBuffer src = this.buffer.duplicate();
        src.rewind();
        newBuffer.put(src);

        // Copy new data
        ByteBuffer dataSrc = data.duplicate();
        newBuffer.put(dataSrc);

        // CRITICAL FIX: Reset position to 0 so the buffer is ready for use/checks
        newBuffer.rewind();

        this.buffer = newBuffer;
        updateLength();
    }

    @Override
    public String toString() {
        return String.format("Packet[Type=0x%02X(%s), Status=0x%02X, Length=%d, SPID=0x%04X, PacketId=%d, Window=0x%02X]",
                getType().getValue(), getType(), getStatus(), getLength(), getSPId(), getId(), getWindow());
    }
}