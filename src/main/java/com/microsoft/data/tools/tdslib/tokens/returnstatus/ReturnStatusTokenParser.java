// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.returnstatus;

import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenParser;
import com.microsoft.data.tools.tdslib.tokens.TokenStreamHandler;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

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
