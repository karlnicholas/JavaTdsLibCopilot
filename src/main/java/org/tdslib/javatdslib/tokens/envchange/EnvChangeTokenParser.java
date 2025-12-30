// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.envchange;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

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
                    if (subType == EnvChangeTokenSubType.PACKET_SIZE) {
                        // Packet size is sent as two 4-byte little-endian integers (new, old).
                        return tokenStreamHandler.readUInt32LE()
                            .thenCompose(newSize -> tokenStreamHandler.readUInt32LE()
                                .thenApply(oldSize -> {
                                    int newPacketSize = ((Long) newSize).intValue();
                                    int oldPacketSize = ((Long) oldSize).intValue();
                                    // Update connection options packet size
                                    try {
                                        tokenStreamHandler.getOptions().setPacketSize(newPacketSize);
                                    } catch (IllegalArgumentException ex) {
                                        // ignore invalid packet size from server
                                    }
                                    return new EnvChangeToken(subType, String.valueOf(oldPacketSize), String.valueOf(newPacketSize));
                                }));
                    }
                    return tokenStreamHandler.readBVarChar()
                        .thenCompose(newValue -> tokenStreamHandler.readBVarChar()
                            .thenApply(oldValue -> {
                                if (subType == EnvChangeTokenSubType.ROUTING) {
                                    try {
                                        tokenStreamHandler.getOptions().setRoutingHint(newValue);
                                    } catch (Exception ex) {
                                        // ignore failures when storing routing hint
                                    }
                                }
                                return new EnvChangeToken(subType, oldValue, newValue);
                            }));
                }));
    }
}