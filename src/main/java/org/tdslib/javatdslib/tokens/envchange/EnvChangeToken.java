package org.tdslib.javatdslib.tokens.envchange;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Represents an ENVCHANGE token (0xE3) sent by the server.
 */
public class EnvChangeToken extends Token {

    private final EnvChangeType changeType;
    private final String oldValue;
    private final String newValue;

    public EnvChangeToken(EnvChangeType changeType, String oldValue, String newValue) {
        this.changeType = changeType != null ? changeType : EnvChangeType.UNKNOWN;
        this.oldValue = oldValue != null ? oldValue.trim() : "";
        this.newValue = newValue != null ? newValue.trim() : "";
    }

    /**
     * Returns the general TDS token type (always ENV_CHANGE).
     */
    @Override
    public TokenType getType() {
        return TokenType.ENV_CHANGE;
    }

    /**
     * Returns the specific subtype of this ENVCHANGE (e.g. PACKET_SIZE, DATABASE, etc.).
     */
    public EnvChangeType getChangeType() {
        return changeType;
    }

    public String getOldValue() {
        return oldValue;
    }

    public String getNewValue() {
        return newValue;
    }

    public int getNewValueAsInt() {
        try {
            return Integer.parseInt(newValue);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    @Override
    public String toString() {
        return "EnvChangeToken{" +
                "changeType=" + changeType +
                ", old='" + oldValue + '\'' +
                ", new='" + newValue + '\'' +
                '}';
    }
}