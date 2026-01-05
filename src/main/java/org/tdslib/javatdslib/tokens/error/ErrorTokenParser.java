package org.tdslib.javatdslib.tokens.error;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Parser for ERROR token (0xAA).
 * Eagerly decodes the full error details.
 */
public class ErrorTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context) {
        // Length (2 bytes) - we ignore it since we parse sequentially
        payload.getShort(); // skip length

        long number = Integer.toUnsignedLong(payload.getInt());
        byte state = payload.get();
        byte severity = payload.get();

        String message = readUsVarChar(payload);
        String serverName = readBVarChar(payload);
        String procName = readBVarChar(payload);

        long lineNumber;
        if (context != null && context.getTdsVersion().ordinal() < TdsVersion.V7_2.ordinal()) {
            lineNumber = Short.toUnsignedInt(payload.getShort());
        } else {
            lineNumber = Integer.toUnsignedLong(payload.getInt());
        }

        return new ErrorToken(
                number,
                state,
                severity,
                message,
                serverName,
                procName,
                lineNumber
        );
    }

    private static String readUsVarChar(ByteBuffer buf) {
        int byteLen = Short.toUnsignedInt(buf.getShort());
        if (byteLen == 0xFFFF || byteLen == 0) return "";
        byte[] bytes = new byte[byteLen];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_16LE).trim();
    }

    private static String readBVarChar(ByteBuffer buf) {
        int len = Byte.toUnsignedInt(buf.get());
        if (len == 0) return "";
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.US_ASCII).trim();
    }
}