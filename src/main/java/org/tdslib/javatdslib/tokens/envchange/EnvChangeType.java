package org.tdslib.javatdslib.tokens.envchange;

/**
 * Known ENVCHANGE type codes from MS-TDS specification.
 */
public enum EnvChangeType {
    PACKET_SIZE((byte) 1),
    DATABASE((byte) 2),
    LANGUAGE((byte) 3),
    PACKET_SIZE_ALT((byte) 4),
    COLLATION((byte) 5),
    BEGIN_TRAN((byte) 6),
    COMMIT_TRAN((byte) 7),
    ROLLBACK_TRAN((byte) 8),
    ANSI_SETTINGS((byte) 9),

    UNKNOWN((byte) -1);

    private final byte value;

    EnvChangeType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static EnvChangeType fromByte(byte b) {
        for (EnvChangeType t : values()) {
            if (t.value == b) {
                return t;
            }
        }
        return UNKNOWN;
    }
}