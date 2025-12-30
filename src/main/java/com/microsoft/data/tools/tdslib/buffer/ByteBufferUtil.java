// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Buffer to handle low-level byte operations using java.nio.ByteBuffer.
 */
public class ByteBufferUtil {

    /**
     * Empty ByteBuffer.
     */
    public static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    /**
     * Creates a new ByteBuffer with the specified capacity.
     */
    public static ByteBuffer allocate(int capacity) {
        return ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Wraps the given byte array into a ByteBuffer.
     */
    public static ByteBuffer wrap(byte[] array) {
        return ByteBuffer.wrap(array).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Wraps the given byte array into a ByteBuffer with specified offset and length.
     */
    public static ByteBuffer wrap(byte[] array, int offset, int length) {
        return ByteBuffer.wrap(array, offset, length).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Reads an Int8 from the buffer at the current position.
     */
    public static byte readInt8(ByteBuffer buffer) {
        return buffer.get();
    }

    /**
     * Reads a UInt8 from the buffer at the current position.
     */
    public static int readUInt8(ByteBuffer buffer) {
        return buffer.get() & 0xFF;
    }

    /**
     * Reads an Int16 in Little Endian from the buffer at the current position.
     */
    public static short readInt16LE(ByteBuffer buffer) {
        return buffer.getShort();
    }

    /**
     * Reads an UInt16 in Little Endian from the buffer at the current position.
     */
    public static int readUInt16LE(ByteBuffer buffer) {
        return buffer.getShort() & 0xFFFF;
    }

    /**
     * Reads an Int32 in Little Endian from the buffer at the current position.
     */
    public static int readInt32LE(ByteBuffer buffer) {
        return buffer.getInt();
    }

    /**
     * Reads an UInt32 in Little Endian from the buffer at the current position.
     */
    public static long readUInt32LE(ByteBuffer buffer) {
        return buffer.getInt() & 0xFFFFFFFFL;
    }

    /**
     * Reads an Int64 in Little Endian from the buffer at the current position.
     */
    public static long readInt64LE(ByteBuffer buffer) {
        return buffer.getLong();
    }

    /**
     * Reads a Float in Little Endian from the buffer at the current position.
     */
    public static float readFloatLE(ByteBuffer buffer) {
        return buffer.getFloat();
    }

    /**
     * Reads a Double in Little Endian from the buffer at the current position.
     */
    public static double readDoubleLE(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    /**
     * Reads a B_VARCHAR (byte length prefix, UTF-16LE string).
     */
    public static String readBVarChar(ByteBuffer buffer) {
        int length = readUInt8(buffer);
        byte[] bytes = new byte[length * 2];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_16LE);
    }

    /**
     * Reads a US_VARCHAR (ushort length prefix, UTF-16LE string).
     */
    public static String readUsVarChar(ByteBuffer buffer) {
        int length = readUInt16LE(buffer);
        byte[] bytes = new byte[length * 2];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_16LE);
    }

    /**
     * Reads a B_VARBYTE (byte length prefix, byte array).
     */
    public static byte[] readBVarByte(ByteBuffer buffer) {
        int length = readUInt8(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * Reads a US_VARBYTE (ushort length prefix, byte array).
     */
    public static byte[] readUsVarByte(ByteBuffer buffer) {
        int length = readUInt16LE(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * Writes an Int8 to the buffer.
     */
    public static void writeInt8(ByteBuffer buffer, byte value) {
        buffer.put(value);
    }

    /**
     * Writes a UInt8 to the buffer.
     */
    public static void writeUInt8(ByteBuffer buffer, int value) {
        buffer.put((byte) value);
    }

    /**
     * Writes an Int16 in Little Endian to the buffer.
     */
    public static void writeInt16LE(ByteBuffer buffer, short value) {
        buffer.putShort(value);
    }

    /**
     * Writes an UInt16 in Little Endian to the buffer.
     */
    public static void writeUInt16LE(ByteBuffer buffer, int value) {
        buffer.putShort((short) value);
    }

    /**
     * Writes an Int32 in Little Endian to the buffer.
     */
    public static void writeInt32LE(ByteBuffer buffer, int value) {
        buffer.putInt(value);
    }

    /**
     * Writes an UInt32 in Little Endian to the buffer.
     */
    public static void writeUInt32LE(ByteBuffer buffer, long value) {
        buffer.putInt((int) value);
    }

    /**
     * Writes an Int64 in Little Endian to the buffer.
     */
    public static void writeInt64LE(ByteBuffer buffer, long value) {
        buffer.putLong(value);
    }

    /**
     * Writes a Float in Little Endian to the buffer.
     */
    public static void writeFloatLE(ByteBuffer buffer, float value) {
        buffer.putFloat(value);
    }

    /**
     * Writes a Double in Little Endian to the buffer.
     */
    public static void writeDoubleLE(ByteBuffer buffer, double value) {
        buffer.putDouble(value);
    }

    /**
     * Writes a B_VARCHAR.
     */
    public static void writeBVarChar(ByteBuffer buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_16LE);
        writeUInt8(buffer, bytes.length / 2);
        buffer.put(bytes);
    }

    /**
     * Writes a US_VARCHAR.
     */
    public static void writeUsVarChar(ByteBuffer buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_16LE);
        writeUInt16LE(buffer, bytes.length / 2);
        buffer.put(bytes);
    }

    /**
     * Writes a B_VARBYTE.
     */
    public static void writeBVarByte(ByteBuffer buffer, byte[] value) {
        writeUInt8(buffer, value.length);
        buffer.put(value);
    }

    /**
     * Writes a US_VARBYTE.
     */
    public static void writeUsVarByte(ByteBuffer buffer, byte[] value) {
        writeUInt16LE(buffer, value.length);
        buffer.put(value);
    }

    /**
     * Reads an Int16 in Big Endian from the buffer at the current position.
     */
    public static short readInt16BE(ByteBuffer buffer) {
        return (short) ((buffer.get() << 8) | (buffer.get() & 0xFF));
    }

    /**
     * Reads an UInt16 in Big Endian from the buffer at the current position.
     */
    public static int readUInt16BE(ByteBuffer buffer) {
        return ((buffer.get() & 0xFF) << 8) | (buffer.get() & 0xFF);
    }

    /**
     * Writes an Int16 in Big Endian to the buffer.
     */
    public static void writeInt16BE(ByteBuffer buffer, short value) {
        buffer.put((byte) (value >> 8));
        buffer.put((byte) value);
    }

    /**
     * Writes an UInt16 in Big Endian to the buffer.
     */
    public static void writeUInt16BE(ByteBuffer buffer, int value) {
        buffer.put((byte) (value >> 8));
        buffer.put((byte) value);
    }
}