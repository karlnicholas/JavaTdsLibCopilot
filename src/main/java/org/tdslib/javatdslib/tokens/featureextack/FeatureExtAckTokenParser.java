package org.tdslib.javatdslib.tokens.featureextack;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

import java.nio.ByteBuffer;

/**
 * Parser for FEATURE_EXT_ACK token (0xAE).
 * Eagerly decodes the feature ID and its associated data.
 */
public class FeatureExtAckTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context, QueryContext queryContext) {
        if (tokenType != TokenType.FEATURE_EXT_ACK.getValue()) {
            throw new IllegalArgumentException(
                    "Expected FEATURE_EXT_ACK token (0xAE), got 0x" + Integer.toHexString(tokenType & 0xFF));
        }

        // Feature extension ID (1 byte)
        byte featureId = payload.get();

        // Remaining bytes are the feature-specific data (variable length)
        int remaining = payload.remaining();
        byte[] data = new byte[remaining];
        payload.get(data);

        return new FeatureExtAckToken(tokenType, featureId, data);
    }
}