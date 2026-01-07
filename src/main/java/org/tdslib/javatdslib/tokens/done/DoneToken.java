package org.tdslib.javatdslib.tokens.done;

/**
 * Standard DONE token (0xFD) - marks end of a batch or statement.
 */
public final class DoneToken extends AbstractDoneToken {

    public DoneToken(byte type, DoneStatus status, int currentCommand, long rowCount) {
        super(type, status, currentCommand, rowCount);
    }

}