// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens;

/**
 * Tds data stream token.
 */
public abstract class Token {

    /**
     * Type of the token.
     */
    public abstract TokenType getType();

}