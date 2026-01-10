package org.tdslib.javatdslib.tokens.featureextack;

import java.nio.ByteBuffer;
import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Parser for FEATURE_EXT_ACK token (0xAE).
 *
 * <p>Eagerly decodes the feature ID and its associated data.</p>
 */
public class FeatureExtAckTokenParser implements TokenParser {

    @Override
    public Token parse(final ByteBuffer payload,
                       final byte tokenType,
                       final ConnectionContext context,
                       final QueryContext queryContext) {
        if (tokenType != TokenType.FEATURE_EXT_ACK.getValue()) {
            final String hex = Integer.toHexString(tokenType & 0xFF);
            throw new IllegalArgumentException(
                    "Expected FEATURE_EXT_ACK token (0xAE), got 0x" + hex
            );
        }

        // Feature extension ID (1 byte)
        byte featureId = payload.get();

        // Remaining bytes are the feature-specific data (variable length)
        int remaining = payload.remaining();
        byte[] data = new byte[remaining];
        if (remaining > 0) {
            payload.get(data);
        }

        return new FeatureExtAckToken(tokenType, featureId, data);
    }
}
