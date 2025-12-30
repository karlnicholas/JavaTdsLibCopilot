// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.returnvalue;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Return value token (skeleton).
 */
public final class ReturnValueToken extends Token {
    private final String name;
    private final byte[] data;

    public ReturnValueToken(String name, byte[] data) {
        this.name = name;
        this.data = data;
    }

    @Override
    public TokenType getType() {
        return TokenType.RETURN_VALUE;
    }

    public String getName() {
        return name;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return "ReturnValueToken[Name=" + name + "]";
    }
}
