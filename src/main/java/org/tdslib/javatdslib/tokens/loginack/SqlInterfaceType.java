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

    public static SqlInterfaceType fromByte(final byte b) {
        for (final SqlInterfaceType t : values()) {
            if (t.value == b) {
                return t;
            }
        }
        return SQL_DFLT; // fallback
    }
}