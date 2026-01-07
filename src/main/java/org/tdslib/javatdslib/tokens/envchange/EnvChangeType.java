package org.tdslib.javatdslib.tokens.envchange;

import java.util.HashMap;
import java.util.Map;

/**
 * Environment change subtypes for ENVCHANGE token (0xE3).
 *
 * Byte values are taken directly from the MS-TDS protocol specification.
 *
 * @see <a href="https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/0b83e8c3-9d5e-4e2f-b8b5-8d4b8e4f8f0d">MS-TDS - ENVCHANGE</a>
 */
public enum EnvChangeType {

    DATABASE((byte) 1, "Database context change"),
    LANGUAGE((byte) 2, "Language change"),
    CHARSET((byte) 3, "Character set change (legacy)"),
    PACKET_SIZE((byte) 4, "Packet size change"),
    UNICODE_LOCAL_ID((byte) 5, "Unicode locale ID"),
    UNICODE_COMPARISON_STYLE((byte) 6, "Unicode comparison style"),
    SQL_COLLATION((byte) 7, "SQL Collation change"),
    BEGIN_TRANSACTION((byte) 8, "Begin transaction"),
    COMMIT_TRANSACTION((byte) 9, "Commit transaction"),
    ROLLBACK_TRANSACTION((byte) 10, "Rollback transaction"),
    ENLIST_DTC_TRANSACTION((byte) 11, "Enlist DTC transaction"),
    DEFECT_TRANSACTION((byte) 12, "Defect transaction"),
    PROMOTE_TRANSACTION((byte) 15, "Promote transaction"),
    TRANSACTION_MANAGER_ADDRESS((byte) 16, "Transaction manager address"),
    TRANSACTION_ENDED((byte) 17, "Transaction ended"),
    RESET_CONNECTION((byte) 18, "Reset connection"),
    RESET_CONNECTION_SKIP_TRAN((byte) 19, "Reset connection (skip transaction)"),
    ROUTING((byte) 20, "Routing information"),

    // Rare/alternative
    PACKET_SIZE_ALT((byte) 0xA7, "Alternative packet size change"),

    // Fallback
    UNKNOWN((byte) -1, "Unknown change type");

    private final byte value;
    private final String description;

    EnvChangeType(byte value, String description) {
        this.value = value;
        this.description = description;
    }

    public byte getValue() {
        return value;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Fast lookup: returns the matching type or UNKNOWN.
     */
    public static EnvChangeType fromByte(byte b) {
        return Lookup.MAP.getOrDefault(b, UNKNOWN);
    }

    @Override
    public String toString() {
        return name() + " (0x" + String.format("%02X", value & 0xFF) + ")";
    }

    private static final class Lookup {
        private static final Map<Byte, EnvChangeType> MAP = new HashMap<>();

        static {
            for (EnvChangeType t : values()) {
                if (t.value != -1) {  // don't map UNKNOWN
                    MAP.put(t.value, t);
                }
            }
        }
    }
}