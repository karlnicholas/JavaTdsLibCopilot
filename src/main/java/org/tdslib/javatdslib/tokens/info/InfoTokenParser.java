package org.tdslib.javatdslib.tokens.info;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Parser for INFO token (0xAB).
 * Eagerly decodes the full informational message details.
 */
public class InfoTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context) {
        if (tokenType != TokenType.INFO.getValue()) {
            throw new IllegalArgumentException("Expected INFO (0xAB), got 0x" + Integer.toHexString(tokenType & 0xFF));
        }

        // Total length of the rest (bytes) — optional skip/validate
        int totalLength = Short.toUnsignedInt(payload.getShort());

        long number = Integer.toUnsignedLong(payload.getInt());
        byte state = payload.get();
        byte severity = payload.get();

        // Message text: USHORT char count → bytes = count * 2
        int msgCharLen = Short.toUnsignedInt(payload.getShort());
        String message = "";
        if (msgCharLen > 0) {
            byte[] msgBytes = new byte[msgCharLen * 2];
            payload.get(msgBytes);
            message = new String(msgBytes, StandardCharsets.UTF_16LE).trim();
        }

        // Server name: BYTE char count → bytes = count * 2
        int serverCharLen = payload.get() & 0xFF;
        String serverName = "";
        if (serverCharLen > 0) {
            byte[] serverBytes = new byte[serverCharLen * 2];
            payload.get(serverBytes);
            serverName = new String(serverBytes, StandardCharsets.UTF_16LE).trim();
        }

        // Proc name: BYTE char count → bytes = count * 2
        int procCharLen = payload.get() & 0xFF;
        String procName = "";
        if (procCharLen > 0) {
            byte[] procBytes = new byte[procCharLen * 2];
            payload.get(procBytes);
            procName = new String(procBytes, StandardCharsets.UTF_16LE).trim();
        }

        // Line number: 2 or 4 bytes depending on TDS version
        long lineNumber;
        if (context != null && context.getTdsVersion().ordinal() < TdsVersion.V7_2.ordinal()) {
            lineNumber = Short.toUnsignedInt(payload.getShort());
        } else {
            lineNumber = Integer.toUnsignedLong(payload.getInt());
        }

        return new InfoToken(tokenType, number, state, severity, message, serverName, procName, lineNumber);
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