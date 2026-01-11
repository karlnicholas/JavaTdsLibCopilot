package org.tdslib.javatdslib.payloads.prelogin;

/**
 * Encryption type.
 */
public enum EncryptionType {
    /**
     * Off.
     */
    OFF(0x00),

    /**
     * On.
     */
    ON(0x01),

    /**
     * Not supported.
     */
    NOT_SUPPORTED(0x02),

    /**
     * Required.
     */
    REQUIRED(0x03);

    private final byte value;

    EncryptionType(final int value) {
        this.value = (byte) value;
    }

    /**
     * Get the byte value representing this encryption type.
     *
     * @return byte representation of the enum value
     */
    public byte getValue() {
        return value;
    }

    /**
     * Resolve an {@link EncryptionType} from a raw byte value.
     * Unknown values default to {@link #OFF}.
     *
     * @param value raw byte value
     * @return corresponding EncryptionType or OFF if unknown
     */
    public static EncryptionType fromValue(final byte value) {
        for (EncryptionType t : values()) {
            if (t.value == value) {
                return t;
            }
        }
        return OFF;
    }
}
