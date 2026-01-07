package org.tdslib.javatdslib.tokens.done;

/**
 * DONE_IN_PROC token (0xFF) - completion of a statement inside a stored procedure.
 */
public final class DoneInProcToken extends AbstractDoneToken {

    public DoneInProcToken(byte type, DoneStatus status, int currentCommand, long rowCount) {
        super(type, status, currentCommand, rowCount);
    }

}