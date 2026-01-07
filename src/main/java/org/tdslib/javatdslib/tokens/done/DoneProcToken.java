package org.tdslib.javatdslib.tokens.done;

/**
 * DONE_PROC token (0xFE) - completion of a stored procedure or RPC.
 */
public final class DoneProcToken extends AbstractDoneToken {

    public DoneProcToken(byte type, DoneStatus status, int currentCommand, long rowCount) {
        super(type, status, currentCommand, rowCount);
    }

}