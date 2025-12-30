// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.loginack;

/**
 * SQL interface type.
 */
public enum SqlInterfaceType {
    /**
     * Default.
     */
    DEFAULT(0),

    /**
     * T-SQL.
     */
    T_SQL(1);

    private final byte value;

    SqlInterfaceType(int value) {
        this.value = (byte) value;
    }

    public byte getValue() {
        return value;
    }

    public static SqlInterfaceType fromValue(byte value) {
        for (SqlInterfaceType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        return null;
    }
}