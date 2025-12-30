// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.done;

/**
 * Indicates the completion status of a SQL statement.
 */
public class DoneToken extends com.microsoft.data.tools.tdslib.tokens.Token {
    private final DoneStatus status;
    private final int currentCommand;
    private final long rowCount;

    /**
     * Token type.
     */
    @Override
    public com.microsoft.data.tools.tdslib.tokens.TokenType getType() {
        return com.microsoft.data.tools.tdslib.tokens.TokenType.DONE;
    }

    /**
     * Status.
     */
    public DoneStatus getStatus() {
        return status;
    }

    /**
     * Current command.
     */
    public int getCurrentCommand() {
        return currentCommand;
    }

    /**
     * Row count.
     */
    public long getRowCount() {
        return rowCount;
    }

    /**
     * Create a new instance with a status, current command and row count.
     */
    public DoneToken(DoneStatus status, int currentCommand, long rowCount) {
        this.status = status;
        this.currentCommand = currentCommand;
        this.rowCount = rowCount;
    }

    /**
     * Gets a human readable string representation of this token.
     */
    @Override
    public String toString() {
        return "DoneToken[Status=0x" + Integer.toHexString(status.getValue()) + "(" + status + "), CurrentCommand=" + currentCommand + ", RowCount=" + rowCount + "]";
    }
}