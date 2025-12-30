// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.fedauth;


import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

import java.util.concurrent.CompletableFuture;

/**
 * Parser for FED_AUTH_INFO token (skeleton, not implemented).
 */
public class FedAuthInfoTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        return tokenStreamHandler.readUInt16LE()
            .thenCompose(length -> tokenStreamHandler.readBytes((int) (long) length)
                .thenApply(bytes -> new FedAuthInfoToken(bytes)));
    }
}
