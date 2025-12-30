// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.envchange;

/**
 * Environment change token sub type.
 */
public enum EnvChangeTokenSubType {
    /**
     * Database.
     */
    DATABASE(1),

    /**
     * Language.
     */
    LANGUAGE(2),

    /**
     * Character set.
     */
    CHARACTER_SET(3),

    /**
     * Packet size.
     */
    PACKET_SIZE(4),

    /**
     * Unicode data sorting local id.
     */
    UNICODE_DATA_SORTING_LOCAL_ID(5),

    /**
     * Unicode data sorting comparison flags.
     */
    UNICODE_DATA_SORTING_COMPARISON_FLAGS(6),

    /**
     * Sql collation.
     */
    SQL_COLLATION(7),

    /**
     * Begin transaction.
     */
    BEGIN_TRANSACTION(8),

    /**
     * Commit transaction.
     */
    COMMIT_TRANSACTION(9),

    /**
     * Rollback transaction.
     */
    ROLLBACK_TRANSACTION(10),

    /**
     * Enlist DTC transaction.
     */
    ENLIST_DTC_TRANSACTION(11),

    /**
     * Defect transaction.
     */
    DEFECT_TRANSACTION(12),

    /**
     * Database mirroring partner (Real time log shipping).
     */
    DATABASE_MIRRORING_PARTNER(13),

    /**
     * Promote transaction.
     */
    PROMOTE_TRANSACTION(15),

    /**
     * Transaction manager address.
     */
    TRANSACTION_MANAGER_ADDRESS(16),

    /**
     * Transaction ended.
     */
    TRANSACTION_ENDED(17),

    /**
     * Reset connection.
     */
    RESET_CONNECTION(18),

    /**
     * User instance name.
     */
    USER_INSTANCE_NAME(19),

    /**
     * Routing.
     */
    ROUTING(20);

    private final byte value;

    EnvChangeTokenSubType(int value) {
        this.value = (byte) value;
    }

    public byte getValue() {
        return value;
    }

    public static EnvChangeTokenSubType fromValue(byte value) {
        for (EnvChangeTokenSubType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        return null;
    }
}