package org.tdslib.javatdslib.tokens.done;

import org.tdslib.javatdslib.tokens.TokenType;

/**
 * DONE_IN_PROC token (0xFF) - completion of a statement inside a stored procedure.
 */
public final class DoneInProcToken extends AbstractDoneToken {

    public DoneInProcToken(DoneStatus status, int currentCommand, long rowCount) {
        super(status, currentCommand, rowCount);
    }

    @Override
    public TokenType getType() {
        return TokenType.DONE_IN_PROC;
    }
}