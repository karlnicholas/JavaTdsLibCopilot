package org.tdslib.javatdslib.tokens.returnstatus;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Return status token (0x79) - contains the return value from a stored procedure or RPC.
 */
public final class ReturnStatusToken extends Token {

    private final int value;

    public ReturnStatusToken(byte tokenType, int value) {
        super(TokenType.fromValue(tokenType));
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "ReturnStatusToken{value=" + value + "}";
    }
}