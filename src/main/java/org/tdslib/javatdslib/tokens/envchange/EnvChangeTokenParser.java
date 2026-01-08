package org.tdslib.javatdslib.tokens.envchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;

import java.nio.ByteBuffer;

/**
 * Parser for ENVCHANGE token (0xE3).
 * Eagerly decodes the token and applies the change to the connection context.
 */
public class EnvChangeTokenParser implements TokenParser {
    private static final Logger logger = LoggerFactory.getLogger(EnvChangeTokenParser.class);

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context, QueryContext queryContext) {
        // Read the total EnvValueData length in bytes (USHORT after the token type 0xE3)
        int envDataLength = payload.getShort() & 0xFFFF;

        // Read the entire remaining payload for this token (type + old/new values + any binary data)
        byte[] allBytes = new byte[envDataLength];
        payload.get(allBytes);

        // The first byte of allBytes is the change type — extract it
        byte changeTypeByte = allBytes[0];
        EnvChangeType changeType = EnvChangeType.fromByte(changeTypeByte);

        // Store the raw bytes (excluding the change type byte itself, or keep it — your choice)
        // Here we keep everything except the type byte for cleaner subtype decoding
        byte[] valueBytes = new byte[envDataLength - 1];
        System.arraycopy(allBytes, 1, valueBytes, 0, valueBytes.length);

        return new EnvChangeToken(tokenType, changeType, valueBytes);
    }

    private int parseIntSafe(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}