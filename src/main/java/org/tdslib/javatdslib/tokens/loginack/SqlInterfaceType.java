package org.tdslib.javatdslib.tokens.loginack;

/**
 * SQL interface type reported by LOGINACK.
 */
public enum SqlInterfaceType {
    SQL_DFLT((byte) 0x01),
    SQL_TSQL((byte) 0x02);

    private final byte value;

    SqlInterfaceType(final byte value) {
        this.value = value;
    }

    /**
     * Returns the {@link SqlInterfaceType} corresponding to the given raw byte
     * value from the LOGINACK payload. If the value is unrecognized, this method
     * returns {@code SQL_DFLT} as a safe fallback.
     *
     * @param b raw interface type byte from LOGINACK
     * @return matching SqlInterfaceType, or SQL_DFLT if not recognized
     */
    public static SqlInterfaceType fromByte(final byte b) {
        for (final SqlInterfaceType t : values()) {
            if (t.value == b) {
                return t;
            }
        }
        return SQL_DFLT; // fallback
    }
}
