package org.tdslib.javatdslib.tokens.done;

import org.tdslib.javatdslib.tokens.TokenType;

/**
 * DONE_PROC token (0xFE) - completion of a stored procedure or RPC.
 */
public final class DoneProcToken extends AbstractDoneToken {

    public DoneProcToken(DoneStatus status, int currentCommand, long rowCount) {
        super(status, currentCommand, rowCount);
    }

    @Override
    public TokenType getType() {
        return TokenType.DONE_PROC;
    }
}