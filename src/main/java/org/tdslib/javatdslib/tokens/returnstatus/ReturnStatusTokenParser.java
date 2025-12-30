// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.returnstatus;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

import java.util.concurrent.CompletableFuture;

/**
 * Parser for RETURN_STATUS token.
 */
public class ReturnStatusTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        return tokenStreamHandler.readUInt32LE()
            .thenApply(v -> new ReturnStatusToken(((Long) v).intValue()));
    }
}
