// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.returnvalue;

import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenParser;
import com.microsoft.data.tools.tdslib.tokens.TokenStreamHandler;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

import java.util.concurrent.CompletableFuture;

/**
 * Parser for RETURN_VALUE token (not yet implemented).
 */
public class ReturnValueTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        return tokenStreamHandler.readUsVarChar()
            .thenCompose(name -> tokenStreamHandler.readUInt32LE()
                .thenCompose(length -> tokenStreamHandler.readBytes(((Long) length).intValue())
                    .thenApply(bytes -> new com.microsoft.data.tools.tdslib.tokens.returnvalue.ReturnValueToken(name, bytes))));
    }
}
