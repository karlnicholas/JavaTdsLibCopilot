// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.featureextack;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

import java.util.concurrent.CompletableFuture;

/**
 * Parser for FEATURE_EXT_ACK token (skeleton, not implemented).
 */
public class FeatureExtAckTokenParser extends TokenParser {
    @Override
    public CompletableFuture<Token> parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        return tokenStreamHandler.readUInt16LE()
            .thenCompose(length -> tokenStreamHandler.readBytes((int) (long) length)
                .thenApply(bytes -> new FeatureExtAckToken(((bytes.length >= 4) ? ((bytes[0] & 0xFF) | ((bytes[1] & 0xFF) << 8) | ((bytes[2] & 0xFF) << 16) | ((bytes[3] & 0xFF) << 24)) : 0))));
    }
}
