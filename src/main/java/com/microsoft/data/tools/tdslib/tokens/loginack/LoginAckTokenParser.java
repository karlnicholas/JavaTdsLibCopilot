// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.loginack;

import com.microsoft.data.tools.tdslib.TdsVersion;
import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenParser;
import com.microsoft.data.tools.tdslib.tokens.TokenStreamHandler;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

import java.util.concurrent.CompletableFuture;

/**
 * Login ack token parser.
 */
public class LoginAckTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        return tokenStreamHandler.readUInt16LE() // length
            .thenCompose(length -> tokenStreamHandler.readUInt8()
                .thenCompose(type -> {
                    SqlInterfaceType interfaceType = SqlInterfaceType.fromValue((byte) (int) type);
                    if (interfaceType == null) {
                        throw new IllegalArgumentException("Unknown Sql Interface type: " + type);
                    }
                    return tokenStreamHandler.readUInt32BE()
                        .thenCompose(version -> {
                            TdsVersion tdsVersion = TdsVersion.fromValue(version.intValue());
                            if (tdsVersion == null) {
                                throw new IllegalArgumentException("Unknown Tds Version: " + Integer.toHexString(version.intValue()));
                            }
                            return tokenStreamHandler.readBVarChar()
                                .thenCompose(progName -> tokenStreamHandler.readUInt8()
                                    .thenCompose(major -> tokenStreamHandler.readUInt8()
                                        .thenCompose(minor -> tokenStreamHandler.readUInt8()
                                            .thenCompose(buildHi -> tokenStreamHandler.readUInt8()
                                                .thenApply(buildLow -> {
                                                    ProgVersion progVersion = new ProgVersion((byte) (int) major, (byte) (int) minor, (byte) (int) buildHi, (byte) (int) buildLow);
                                                    return new LoginAckToken(interfaceType, tdsVersion, progName, progVersion);
                                                })))));
                        });
                }));
    }
}