package org.tdslib.javatdslib.tokens.loginack;

/**
 * Known SQL Server product versions from LOGINACK token.
 * Values are the little-endian 32-bit integer from the wire.
 */
public enum ServerVersion {
    /**
     * SQL Server 2019 (15.0)
     */
    SQL_SERVER_2019(0x0F000000),

    /**
     * SQL Server 2022 (16.0)
     */
    SQL_SERVER_2022(0x10000000),

    /**
     * SQL Server 2025 (17.0)  ← new
     */
    SQL_SERVER_2025(0x11000000),

    /**
     * SQL Server 2017 (14.0)
     */
    SQL_SERVER_2017(0x0E000000),

    /**
     * SQL Server 2016 (13.0)
     */
    SQL_SERVER_2016(0x0D000000),

    /**
     * SQL Server 2014 (12.0)
     */
    SQL_SERVER_2014(0x0C000000),

    /**
     * SQL Server 2012 (11.0)
     */
    SQL_SERVER_2012(0x0B000000),

    /**
     * Fallback/Unknown version
     */
    UNKNOWN(0x00000000);

    private final int value;

    ServerVersion(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static ServerVersion fromValue(int value) {
        // Mask to major.minor only (high 16 bits)
        int majorMinor = value & 0xFFFF0000;
        for (ServerVersion v : values()) {
            if (v.value == majorMinor) {
                return v;
            }
        }
        return UNKNOWN;
    }

    /**
     * Returns major version number.
     */
    public int getMajor() {
        return (value >> 24) & 0xFF;
    }

    /**
     * Returns minor version number.
     */
    public int getMinor() {
        return (value >> 16) & 0xFF;
    }

    /**
     * Returns human-readable version string (major.minor).
     */
    public String toVersionString() {
        return getMajor() + "." + getMinor();
    }

    @Override
    public String toString() {
        return name() + " (" + toVersionString() + ")";
    }
}