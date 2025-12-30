package com.microsoft.data.tools.tdslib.buffer;

/**
 * Enum representing byte order (endianness).
 * This is a Java equivalent for the C# System.BitConverter.IsLittleEndian usage.
 */
public enum ByteOrder {
    LITTLE_ENDIAN,
    BIG_ENDIAN;

    public static ByteOrder nativeOrder() {
        return java.nio.ByteOrder.nativeOrder() == java.nio.ByteOrder.LITTLE_ENDIAN ? LITTLE_ENDIAN : BIG_ENDIAN;
    }
}
