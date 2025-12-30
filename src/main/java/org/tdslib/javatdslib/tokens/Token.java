// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens;

/**
 * Tds data stream token.
 */
public abstract class Token {

    /**
     * Type of the token.
     */
    public abstract TokenType getType();

}