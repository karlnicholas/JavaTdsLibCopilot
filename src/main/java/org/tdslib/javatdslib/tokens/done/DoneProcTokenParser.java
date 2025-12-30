// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.done;

import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

import java.util.concurrent.CompletableFuture;

/**
 * DoneProc token parser.
 */
public class DoneProcTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        return tokenStreamHandler.readUInt16LE()
            .thenCompose(statusValue -> {
                DoneStatus doneStatus = DoneStatus.fromValue(statusValue);
                return tokenStreamHandler.readUInt16LE()
                    .thenCompose(currentCommand -> {
                        if (tokenStreamHandler.getOptions().getTdsVersion().ordinal() > TdsVersion.V7_2.ordinal()) {
                            return tokenStreamHandler.readUInt64LE()
                                .thenApply(rowCount -> new DoneProcToken(doneStatus, currentCommand, rowCount));
                        } else {
                            return tokenStreamHandler.readUInt32LE()
                                .thenApply(rowCount -> new DoneProcToken(doneStatus, currentCommand, rowCount));
                        }
                    });
            });
    }
}
