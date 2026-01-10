package org.tdslib.javatdslib.tokens.returnstatus;

import java.nio.ByteBuffer;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Parser for RETURN_STATUS token (0x79).
 *
 * <p>Eagerly decodes the 4-byte return value.</p>
 */
public class ReturnStatusTokenParser implements TokenParser {

    @Override
    public Token parse(final ByteBuffer payload,
            final byte tokenType,
            final ConnectionContext context,
            final QueryContext queryContext) {
        if (tokenType != TokenType.RETURN_STATUS.getValue()) {
            final String hex = Integer.toHexString(tokenType & 0xFF);
            throw new IllegalArgumentException(
                    "Expected RETURN_STATUS token (0x79), got 0x" + hex);
        }

        // Return status is always a 4-byte signed integer (little-endian)
        final int value = payload.getInt();

        return new ReturnStatusToken(tokenType, value);
    }
}