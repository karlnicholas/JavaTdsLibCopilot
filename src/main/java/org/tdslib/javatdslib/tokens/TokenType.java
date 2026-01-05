package org.tdslib.javatdslib.tokens;

import java.util.HashMap;
import java.util.Map;

/**
 * TDS Protocol Token Types (as defined in the Tabular Data Stream protocol).
 * <p>
 * Each constant represents a specific token type identifier in the TDS data stream.
 * New token types may be added in future TDS versions — this enum is designed for easy extension.
 *
 * @see <a href="https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds">MS-TDS</a>
 */
public enum TokenType {

    ALT_METADATA((byte) 0x88, "Alternative metadata"),
    ALT_ROW((byte) 0xD3, "Alternative row data"),
    COL_METADATA((byte) 0x81, "Column metadata"),
    COL_INFO((byte) 0xA5, "Column information (browse mode)"),
    DONE((byte) 0xFD, "Done (end of batch/statement)"),
    DONE_IN_PROC((byte) 0xFF, "Done in procedure"),
    DONE_PROC((byte) 0xFE, "Done procedure (RPC)"),
    ENV_CHANGE((byte) 0xE3, "Environment change notification"),
    ERROR((byte) 0xAA, "Error message"),
    FEATURE_EXT_ACK((byte) 0xAE, "Feature extension acknowledgment"),
    FED_AUTH_INFO((byte) 0xEE, "Federated authentication information"),
    INFO((byte) 0xAB, "Informational message"),
    LOGIN_ACK((byte) 0xAD, "Login acknowledgment"),
    NBC_ROW((byte) 0xD2, "Null Bitmap Compressed row"),
    OFFSET((byte) 0x78, "Offset token (cursor)"),
    ORDER((byte) 0xA9, "Order (sort order)"),
    RETURN_STATUS((byte) 0x79, "Return status (procedure)"),
    RETURN_VALUE((byte) 0xAC, "Return value (output param)"),
    ROW((byte) 0xD1, "Standard row data"),
    SSPI((byte) 0xED, "Security Support Provider Interface (SSPI)"),
    TAB_NAME((byte) 0xA4, "Table name");

    private final byte value;
    private final String description;

    TokenType(byte value, String description) {
        this.value = value;
        this.description = description;
    }

    /**
     * Gets the TDS byte value for this token type.
     */
    public byte getValue() {
        return value;
    }

    /**
     * Gets a human-readable description of this token type.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Checks if the given byte is a known token type.
     * <p>
     * This is faster than looping over values() for frequent checks.
     */
    public static boolean isDefined(byte value) {
        return fromValue(value) != null;
    }

    /**
     * Returns the TokenType for the given byte value, or {@code null} if unknown.
     * <p>
     * Uses a reverse lookup map internally for O(1) performance.
     */
    public static TokenType fromValue(byte value) {
        return Lookup.MAP.get(value);
    }

    /**
     * Returns a formatted string with the enum name and hex value.
     */
    @Override
    public String toString() {
        return name() + " (0x" + String.format("%02X", value) + ")";
    }

    /**
     * Internal static lookup for fast fromValue() — O(1) instead of O(n)
     */
    private static final class Lookup {
        private static final Map<Byte, TokenType> MAP = new HashMap<>();

        static {
            for (TokenType type : values()) {
                MAP.put(type.value, type);
            }
        }
    }
}