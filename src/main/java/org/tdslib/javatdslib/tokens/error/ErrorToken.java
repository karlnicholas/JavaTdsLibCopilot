// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


package org.tdslib.javatdslib.tokens.error;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Error message.
 */
public class ErrorToken extends Token {
    private final long number;
    private final byte state;
    private final byte severity;
    private final String message;
    private final String serverName;
    private final String procName;
    private final long lineNumber;

    /**
     * Token type.
     */
    @Override
    public TokenType getType() {
        return TokenType.ERROR;
    }

    /**
     * Error number.
     */
    public long getNumber() {
        return number;
    }

    /**
     * State.
     */
    public byte getState() {
        return state;
    }

    /**
     * Severity.
     */
    public byte getSeverity() {
        return severity;
    }

    /**
     * Message.
     */
    public String getMessage() {
        return message;
    }

    /**
     * Server name.
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * Process name.
     */
    public String getProcName() {
        return procName;
    }

    /**
     * Line number.
     */
    public long getLineNumber() {
        return lineNumber;
    }

    /**
     * Creates a new instance of the token.
     */
    public ErrorToken(long number, byte state, byte severity, String message, String serverName, String procName, long lineNumber) {
        this.number = number;
        this.state = state;
        this.severity = severity;
        this.message = message;
        this.serverName = serverName;
        this.procName = procName;
        this.lineNumber = lineNumber;
    }

    /**
     * Gets a human readable string representation of this token.
     */
    @Override
    public String toString() {
        return "ErrorToken[Number=" + number + ", State=" + state + ", Severity=" + severity + ", Message=" + message + ", ServerName=" + serverName + ", ProcName=" + procName + ", LineNumber=" + lineNumber + "]";
    }
}