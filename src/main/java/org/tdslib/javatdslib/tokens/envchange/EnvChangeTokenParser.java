package org.tdslib.javatdslib.tokens.envchange;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Parser for ENVCHANGE token (0xE3).
 * Eagerly decodes the token and applies the change to the connection context.
 */
public class EnvChangeTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context) {
        if (tokenType != TokenType.ENV_CHANGE.getValue()) {
            throw new IllegalArgumentException(
                    "Expected ENV_CHANGE token (0xE3), got 0x" + Integer.toHexString(tokenType & 0xFF));
        }

        // Type of change
        byte changeTypeByte = payload.get();
        EnvChangeType changeType = EnvChangeType.fromByte(changeTypeByte);

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

    private void applyChange(EnvChangeType type, String newValue, ConnectionContext context) {
        switch (type) {
            case PACKET_SIZE, PACKET_SIZE_ALT:
                int newSize = parseIntSafe(newValue);
                if (newSize >= 512 && newSize <= 32767) {
                    context.setPacketSize(newSize);
                }
                break;
            case DATABASE:
                context.setDatabase(newValue);
                break;
            case UNKNOWN:
                // Log or ignore unknown types
                break;
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