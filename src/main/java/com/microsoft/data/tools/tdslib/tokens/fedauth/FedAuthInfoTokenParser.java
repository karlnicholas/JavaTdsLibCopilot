// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.fedauth;

import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenParser;
import com.microsoft.data.tools.tdslib.tokens.TokenStreamHandler;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

import java.util.concurrent.CompletableFuture;

/**
 * Parser for FED_AUTH_INFO token (skeleton, not implemented).
 */
public class FedAuthInfoTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        return tokenStreamHandler.readUInt16LE()
            .thenCompose(length -> tokenStreamHandler.readBytes((int) (long) length)
                .thenApply(bytes -> new com.microsoft.data.tools.tdslib.tokens.fedauth.FedAuthInfoToken(bytes)));
    }
}
