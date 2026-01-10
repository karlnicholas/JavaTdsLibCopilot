package org.tdslib.javatdslib.tokens.returnstatus;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Return status token (0x79) - contains the return value from a stored procedure or RPC.
 */
public final class ReturnStatusToken extends Token {

    private final int value;

    /**
     * Constructs a ReturnStatusToken.
     *
     * @param tokenType raw token type byte
     * @param value     signed 4-byte return value
     */
    public ReturnStatusToken(final byte tokenType, final int value) {
        super(TokenType.fromValue(tokenType));
        this.value = value;
    }

    /** Returns the return status value. */
    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("ReturnStatusToken{value=%d}", value);
    }
}