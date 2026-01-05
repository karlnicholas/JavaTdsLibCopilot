package org.tdslib.javatdslib.tokens.info;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * INFO token (0xAB) - informational message from the server (severity <= 10).
 */
public final class InfoToken extends Token {

    private final long number;
    private final byte state;
    private final byte severity;
    private final String message;
    private final String serverName;
    private final String procName;
    private final long lineNumber;

    public InfoToken(
            long number,
            byte state,
            byte severity,
            String message,
            String serverName,
            String procName,
            long lineNumber) {

        this.number = number;
        this.state = state;
        this.severity = severity;
        this.message = message != null ? message.trim() : "";
        this.serverName = serverName != null ? serverName.trim() : "";
        this.procName = procName != null ? procName.trim() : "";
        this.lineNumber = lineNumber;
    }

    @Override
    public TokenType getType() {
        return TokenType.INFO;
    }

    public long getNumber() {
        return number;
    }

    public byte getState() {
        return state;
    }

    public byte getSeverity() {
        return severity;
    }

    public String getMessage() {
        return message;
    }

    public String getServerName() {
        return serverName;
    }

    public String getProcName() {
        return procName;
    }

    public long getLineNumber() {
        return lineNumber;
    }

    @Override
    public String toString() {
        return "InfoToken{" +
                "number=" + number +
                ", severity=" + severity +
                ", state=" + state +
                ", message='" + message + '\'' +
                ", server='" + serverName + '\'' +
                ", proc='" + procName + '\'' +
                ", line=" + lineNumber +
                '}';
    }
}