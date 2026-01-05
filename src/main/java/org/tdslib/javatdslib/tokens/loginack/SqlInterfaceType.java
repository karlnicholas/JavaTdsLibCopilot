// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.loginack;

// SqlInterfaceType.java
public enum SqlInterfaceType {
    SQL_DFLT((byte) 0x01),
    SQL_TSQL((byte) 0x02);

    private final byte value;

    SqlInterfaceType(byte value) {
        this.value = value;
    }

    public static SqlInterfaceType fromByte(byte b) {
        for (SqlInterfaceType t : values()) {
            if (t.value == b) return t;
        }
        return SQL_DFLT; // fallback
    }
}