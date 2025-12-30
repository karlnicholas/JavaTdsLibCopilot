// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.packets;

/**
 * Type of Packet.
 */
public enum PacketType {
    /**
     * Unknown packet type.
     */
    UNKNOWN((byte) 0x00),

    /**
     * SQL batch.
     */
    SQL_BATCH((byte) 0x01),

    /**
     * RPC.
     */
    RPC_REQUEST((byte) 0x03),

    /**
     * Tabular result.
     */
    TABULAR_RESULT((byte) 0x04),

    /**
     * Attention signal.
     */
    ATTENTION((byte) 0x06),

    /**
     * Bulk load data.
     */
    BULK_LOAD((byte) 0x07),

    /**
     * Federated Authentication Token.
     */
    FED_AUTH_TOKEN((byte) 0x08),

    /**
     * Transaction manager request.
     */
    TRANSACTION_MANAGER((byte) 0x0E),

    /**
     * TDS7 Login.
     */
    LOGIN7((byte) 0x10),

    /**
     * SSPI (Security Support Provider Interface).
     */
    SSPI((byte) 0x11),

    /**
     * Pre-Login.
     */
    PRE_LOGIN((byte) 0x12);

    private final byte value;

    PacketType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }
}