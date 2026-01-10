package org.tdslib.javatdslib.tokens.done;

/**
 * DONE_IN_PROC token (0xFF) - completion of a statement inside a stored procedure.
 */
public final class DoneInProcToken extends AbstractDoneToken {

    /**
     * Construct a DONE_IN_PROC token.
     *
     * @param type           raw token byte value
     * @param status         DONE status flags
     * @param currentCommand command id associated with the token
     * @param rowCount       row count reported by the token
     */
    public DoneInProcToken(final byte type,
            final DoneStatus status,
            final int currentCommand,
            final long rowCount) {
        super(type, status, currentCommand, rowCount);
    }

}