// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.fedauth;

import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

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
