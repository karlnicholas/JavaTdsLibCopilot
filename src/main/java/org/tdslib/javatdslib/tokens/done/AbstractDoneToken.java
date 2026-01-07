package org.tdslib.javatdslib.tokens.done;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Abstract base class for all DONE family tokens (DONE, DONE_IN_PROC, DONE_PROC).
 * Contains shared fields and logic.
 */
public abstract class AbstractDoneToken extends Token {

    private final DoneStatus status;
    private final int currentCommand;
    private final long rowCount;

    protected AbstractDoneToken(byte type, DoneStatus status, int currentCommand, long rowCount) {
        super(TokenType.fromValue(type));
        this.status = status != null ? status : DoneStatus.FINAL;
        this.currentCommand = currentCommand;
        this.rowCount = rowCount;
    }

    public DoneStatus getStatus() {
        return status;
    }

    public int getCurrentCommand() {
        return currentCommand;
    }

    public long getRowCount() {
        return rowCount;
    }

    public boolean isFinal() {
        return status == DoneStatus.FINAL || !DoneStatus.MORE.isSet(status.getValue());
    }

    public boolean hasError() {
        return DoneStatus.ERROR.isSet(status.getValue());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "status=" + status +
                ", cmd=" + currentCommand +
                ", rows=" + rowCount +
                '}';
    }
}