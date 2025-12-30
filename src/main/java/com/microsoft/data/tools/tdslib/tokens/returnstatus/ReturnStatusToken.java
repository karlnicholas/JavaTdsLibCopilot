// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.returnstatus;

import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

/**
 * Return status token.
 */
public final class ReturnStatusToken extends Token {
    private final int value;

    public ReturnStatusToken(int value) {
        this.value = value;
    }

    @Override
    public TokenType getType() {
        return TokenType.RETURN_STATUS;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "ReturnStatusToken[Value=" + value + "]";
    }
}
