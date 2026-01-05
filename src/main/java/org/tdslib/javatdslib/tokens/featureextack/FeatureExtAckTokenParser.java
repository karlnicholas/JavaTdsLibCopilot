// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.featureextack;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Parser for FEATURE_EXT_ACK token (skeleton, not implemented).
 */
public class FeatureExtAckTokenParser extends TokenParser {

    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        // Read Length (2 bytes)
        int length = tokenStreamHandler.readUInt16LE();

        // Read the raw payload bytes
        byte[] bytes = tokenStreamHandler.readBytes(length);

        // Parse the first 4 bytes as a Little-Endian integer (simulating the skeleton logic)
        int featureData = 0;
        if (bytes.length >= 4) {
            featureData = (bytes[0] & 0xFF) |
                    ((bytes[1] & 0xFF) << 8) |
                    ((bytes[2] & 0xFF) << 16) |
                    ((bytes[3] & 0xFF) << 24);
        }

        return new FeatureExtAckToken(featureData);
    }
}