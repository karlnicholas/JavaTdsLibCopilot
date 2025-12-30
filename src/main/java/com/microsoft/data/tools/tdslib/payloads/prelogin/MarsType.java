package com.microsoft.data.tools.tdslib.payloads.prelogin;

/**
 * MARS (Multiple Active Result Sets) type for PreLogin payload.
 */
public enum MarsType {
    DISABLED((byte)0x00),
    ENABLED((byte)0x01);

    private final byte value;
    MarsType(byte value) { this.value = value; }
    public byte getValue() { return value; }
    public static MarsType fromValue(byte value) {
        for (MarsType t : values()) if (t.value == value) return t;
        return DISABLED;
    }
}
