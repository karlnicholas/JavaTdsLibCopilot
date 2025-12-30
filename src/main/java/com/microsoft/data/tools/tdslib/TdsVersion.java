// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib;

/**
 * TDS Protocol versions.
 */
public enum TdsVersion {
    /**
     * Version 7.1
     */
    V7_1(0x71000001),

    /**
     * Version 7.2
     */
    V7_2(0x72090002),

    /**
     * Version 7.3.A
     */
    V7_3_A(0x730A0003),

    /**
     * Version 7.3.B
     */
    V7_3_B(0x730B0003),

    /**
     * Version 7.4
     */
    V7_4(0x74000004);

    private final int value;

    TdsVersion(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static TdsVersion fromValue(int value) {
        for (TdsVersion version : values()) {
            if (version.value == value) {
                return version;
            }
        }
        return null;
    }
}