// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.payloads.prelogin;

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

    EncryptionType(int value) {
        this.value = (byte) value;
    }

    public byte getValue() {
        return value;
    }
}