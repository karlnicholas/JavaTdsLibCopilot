// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.envchange;

import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenParser;
import com.microsoft.data.tools.tdslib.tokens.TokenStreamHandler;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

import java.util.concurrent.CompletableFuture;

/**
 * Environment change token parser.
 */
public class EnvChangeTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        return tokenStreamHandler.readUInt16LE() // length
            .thenCompose(length -> tokenStreamHandler.readUInt8()
                .thenCompose(subTypeValue -> {
                    EnvChangeTokenSubType subType = EnvChangeTokenSubType.fromValue((byte) (int) subTypeValue);
                    if (subType == null) {
                        throw new IllegalArgumentException("Unknown EnvChange sub type: " + subTypeValue);
                    }
                    return tokenStreamHandler.readBVarChar()
                        .thenCompose(newValue -> tokenStreamHandler.readBVarChar()
                            .thenApply(oldValue -> new EnvChangeToken(subType, oldValue, newValue)));
                }));
    }
}