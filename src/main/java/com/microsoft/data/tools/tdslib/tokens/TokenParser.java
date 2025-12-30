// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens;

import java.util.concurrent.CompletableFuture;

/**
 * Token parser.
 */
public abstract class TokenParser {

    /**
     * Parse a token from the token handler.
     */
    public abstract CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler);
}