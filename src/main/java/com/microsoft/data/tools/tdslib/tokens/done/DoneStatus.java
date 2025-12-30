// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.done;

/**
 * Done status for Done, DoneProc, DoneInProc.
 */
public enum DoneStatus {
    /**
     * Final.
     */
    FINAL(0x0000),

    /**
     * More.
     */
    MORE(0x0001),

    /**
     * Error.
     */
    ERROR(0x0002),

    /**
     * In Transaction.
     */
    IN_XACT(0x0004),

    /**
     * Count.
     */
    COUNT(0x0010),

    /**
     * Attention.
     */
    ATTN(0x0020),

    /**
     * Server Error.
     */
    SERVER_ERROR(0x0100);

    private final int value;

    DoneStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static DoneStatus fromValue(int value) {
        for (DoneStatus status : values()) {
            if (status.value == value) {
                return status;
            }
        }
        return null;
    }
}