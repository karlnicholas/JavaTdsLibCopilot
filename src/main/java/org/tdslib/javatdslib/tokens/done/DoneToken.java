package org.tdslib.javatdslib.tokens.done;

import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Standard DONE token (0xFD) - marks end of a batch or statement.
 */
public final class DoneToken extends AbstractDoneToken {

    public DoneToken(DoneStatus status, int currentCommand, long rowCount) {
        super(status, currentCommand, rowCount);
    }

    @Override
    public TokenType getType() {
        return TokenType.DONE;
    }
}