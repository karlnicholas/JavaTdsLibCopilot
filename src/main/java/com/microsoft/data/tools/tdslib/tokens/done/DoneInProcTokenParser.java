// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.done;

import com.microsoft.data.tools.tdslib.TdsVersion;
import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenParser;
import com.microsoft.data.tools.tdslib.tokens.TokenStreamHandler;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

import java.util.concurrent.CompletableFuture;

/**
 * DoneInProc token parser.
 */
public class DoneInProcTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        return tokenStreamHandler.readUInt16LE()
            .thenCompose(statusValue -> {
                DoneStatus doneStatus = DoneStatus.fromValue(statusValue);
                return tokenStreamHandler.readUInt16LE()
                    .thenCompose(currentCommand -> {
                        if (tokenStreamHandler.getOptions().getTdsVersion().ordinal() > TdsVersion.V7_2.ordinal()) {
                            return tokenStreamHandler.readUInt64LE()
                                .thenApply(rowCount -> new DoneInProcToken(doneStatus, currentCommand, rowCount));
                        } else {
                            return tokenStreamHandler.readUInt32LE()
                                .thenApply(rowCount -> new DoneInProcToken(doneStatus, currentCommand, rowCount));
                        }
                    });
            });
    }
}
