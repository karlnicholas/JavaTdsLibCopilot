// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens;

/**
 * Tds data stream token.
 */
public abstract class Token {
    private final TokenType type;

    protected Token(TokenType type) {
        this.type = type;
    }

    /**
     * Type of the token.
     */
    public final TokenType getType() {
        return type;
    }

}