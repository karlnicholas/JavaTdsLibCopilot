package org.tdslib.javatdslib.tokens.returnstatus;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

import java.nio.ByteBuffer;

/**
 * Parser for RETURN_STATUS token (0x79).
 * Eagerly decodes the 4-byte return value.
 */
public class ReturnStatusTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context) {
        if (tokenType != TokenType.RETURN_STATUS.getValue()) {
            throw new IllegalArgumentException(
                    "Expected RETURN_STATUS token (0x79), got 0x" + Integer.toHexString(tokenType & 0xFF));
        }

        // Return status is always a 4-byte signed integer (little-endian)
        int value = payload.getInt();

        return new ReturnStatusToken(value);
    }
}