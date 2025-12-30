// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.fedauth;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Federated authentication information token (skeleton).
 */
public final class FedAuthInfoToken extends Token {
    private final byte[] data;

    public FedAuthInfoToken(byte[] data) {
        this.data = data;
    }

    @Override
    public TokenType getType() {
        return TokenType.FED_AUTH_INFO;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return "FedAuthInfoToken[Length=" + (data == null ? 0 : data.length) + "]";
    }
}
