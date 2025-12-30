// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.returnvalue;


import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

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
                    .thenApply(bytes -> new ReturnValueToken(name, bytes))));
    }
}
