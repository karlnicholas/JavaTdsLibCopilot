package org.tdslib.javatdslib.tokens.envchange;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Parser for ENVCHANGE token (0xE3).
 * Eagerly decodes the token and applies the change to the connection context.
 */
public class EnvChangeTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context) {
        // Type of change
        byte changeType = payload.get();

        // Old value length + data
        byte oldLen = payload.get();
        byte[] oldData = new byte[oldLen & 0xFF];
        payload.get(oldData);
        String oldValue = new String(oldData, StandardCharsets.US_ASCII).trim();

        // New value length + data
        byte newLen = payload.get();
        byte[] newData = new byte[newLen & 0xFF];
        payload.get(newData);
        String newValue = new String(newData, StandardCharsets.US_ASCII).trim();

        EnvChangeToken token = new EnvChangeToken(changeType, oldValue, newValue);

        // Apply the change immediately to connection context
        applyChange(changeType, newValue, context);

        return token;
    }

    private void applyChange(byte type, String newValue, ConnectionContext context) {
        switch (EnvChangeType.fromByte(type)) {
            case PACKET_SIZE, PACKET_SIZE_ALT -> context.setPacketSize(Integer.parseInt(newValue));
            case DATABASE -> context.setDatabase(newValue);
            case UNKNOWN -> { /* log or ignore */ }
        }
    }

    private int parseIntSafe(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}