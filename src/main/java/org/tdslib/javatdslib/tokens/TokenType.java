// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens;

/**
 * Token type.
 */
public enum TokenType {
    /**
     * Column data format.
     */
    ALT_METADATA((byte) 0x88),

    /**
     * Row of data.
     */
    ALT_ROW((byte) 0xD3),

    /**
     * Column metadata.
     */
    COL_METADATA((byte) 0x81),

    /**
     * Column information in browse mode.
     */
    COL_INFO((byte) 0xA5),

    /**
     * Done.
     */
    DONE((byte) 0xFD),

    /**
     * Procedure done.
     */
    DONE_PROC((byte) 0xFE),

    /**
     * Done in procedure.
     */
    DONE_IN_PROC((byte) 0xFF),

    /**
     * Environment change.
     */
    ENV_CHANGE((byte) 0xE3),

    /**
     * Error.
     */
    ERROR((byte) 0xAA),

    /**
     * Feature extension acknowledgment.
     */
    FEATURE_EXT_ACK((byte) 0xAE),

    /**
     * Federated authentication information.
     */
    FED_AUTH_INFO((byte) 0xEE),

    /**
     * Info.
     */
    INFO((byte) 0xAB),

    /**
     * Login acknowledgment.
     */
    LOGIN_ACK((byte) 0xAD),

    /**
     * Row with Null Bitmap Compression.
     */
    NBC_ROW((byte) 0xD2),

    /**
     * Offset.
     */
    OFFSET((byte) 0x78),

    /**
     * Order.
     */
    ORDER((byte) 0xA9),

    /**
     * Return status.
     */
    RETURN_STATUS((byte) 0x79),

    /**
     * Return value.
     */
    RETURN_VALUE((byte) 0xAC),

    /**
     * Complete Row.
     */
    ROW((byte) 0xD1),

    /**
     * SSPI.
     */
    SSPI((byte) 0xED),

    /**
     * Table name.
     */
    TAB_NAME((byte) 0xA4);

    private final byte value;

    TokenType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static TokenType fromValue(byte value) {
        for (TokenType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        return null;
    }
}