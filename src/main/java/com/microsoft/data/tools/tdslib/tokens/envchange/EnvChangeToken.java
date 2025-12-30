// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


package com.microsoft.data.tools.tdslib.tokens.envchange;

import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

/**
 * Environment change token.
 */
public class EnvChangeToken extends Token {
    private final EnvChangeTokenSubType subType;
    private final String oldValue;
    private final String newValue;

    /**
     * Token type.
     */
    @Override
    public TokenType getType() {
        return TokenType.ENV_CHANGE;
    }

    /**
     * Token sub type.
     */
    public EnvChangeTokenSubType getSubType() {
        return subType;
    }

    /**
     * Old value.
     */
    public String getOldValue() {
        return oldValue;
    }

    /**
     * New value.
     */
    public String getNewValue() {
        return newValue;
    }

    /**
     * Create a new token.
     */
    public EnvChangeToken(EnvChangeTokenSubType subType, String oldValue, String newValue) {
        this.subType = subType;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    /**
     * Gets a human readable string representation of this token.
     */
    @Override
    public String toString() {
        return "EnvChangeToken[SubType=" + subType + ", NewValue=" + newValue + ", OldValue=" + oldValue + "]";
    }
}