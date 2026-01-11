package org.tdslib.javatdslib.payloads.prelogin;

/**
 * MARS (Multiple Active Result Sets) type for PreLogin payload.
 */
public enum MarsType {
    DISABLED((byte) 0x00),
    ENABLED((byte) 0x01);

    private final byte value;

    MarsType(final byte value) {
        this.value = value;
    }

    /**
     * Get the byte value for this MARS setting.
     *
     * @return byte representation
     */
    public byte getValue() {
        return value;
    }

    /**
     * Resolve a MarsType from its byte value.
     * Unknown values default to DISABLED.
     *
     * @param value byte value
     * @return corresponding MarsType or DISABLED if unknown
     */
    public static MarsType fromValue(final byte value) {
        for (MarsType t : values()) {
            if (t.value == value) {
                return t;
            }
        }
        return DISABLED;
    }
}
