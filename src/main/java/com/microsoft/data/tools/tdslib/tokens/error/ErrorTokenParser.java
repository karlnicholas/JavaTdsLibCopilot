// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.error;

import com.microsoft.data.tools.tdslib.TdsVersion;
import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenParser;
import com.microsoft.data.tools.tdslib.tokens.TokenStreamHandler;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

import java.util.concurrent.CompletableFuture;

/**
 * Error token parser.
 */
public class ErrorTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        return tokenStreamHandler.readUInt16LE()
            .thenCompose(length -> tokenStreamHandler.readUInt32LE())
            .thenCompose(number -> tokenStreamHandler.readUInt8()
                .thenCompose(state -> tokenStreamHandler.readUInt8()
                    .thenCompose(severity -> tokenStreamHandler.readUsVarChar()
                        .thenCompose(message -> tokenStreamHandler.readBVarChar()
                            .thenCompose(serverName -> tokenStreamHandler.readBVarChar()
                                .thenCompose(procName -> {
                                    if (tokenStreamHandler.getOptions().getTdsVersion().ordinal() < TdsVersion.V7_2.ordinal()) {
                                        return tokenStreamHandler.readUInt16LE()
                                            .thenApply(lineNumber -> new ErrorToken(number, (byte) (int) state, (byte) (int) severity, message, serverName, procName, lineNumber));
                                    } else {
                                        return tokenStreamHandler.readUInt32LE()
                                            .thenApply(lineNumber -> new ErrorToken(number, (byte) (int) state, (byte) (int) severity, message, serverName, procName, lineNumber));
                                    }
                                }))))));
    }
}