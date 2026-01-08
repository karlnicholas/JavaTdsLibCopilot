package org.tdslib.javatdslib.tokens.error;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ErrorTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context, QueryContext queryContext) {
        if (tokenType != TokenType.ERROR.getValue()) {
            throw new IllegalArgumentException("Wrong token type: 0x" + String.format("%02X", tokenType));
        }

        int start = payload.position();
        int tokenLen = Short.toUnsignedInt(payload.getShort());

        if (tokenLen < 11) throw new IllegalStateException("Token too short: " + tokenLen);

        int end = payload.position() + tokenLen;
        if (end > payload.limit()) throw new IllegalStateException("Token overflows buffer");

        long number = Integer.toUnsignedLong(payload.getInt());
        byte state = payload.get();
        byte severity = payload.get();

        String message    = readUsVarChar(payload, end);
        String serverName = readBVarChar(payload, end);
        String procName   = readBVarChar(payload, end);

        long lineNumber = (context != null && context.getTdsVersion().ordinal() < TdsVersion.V7_2.ordinal())
                ? Short.toUnsignedInt(payload.getShort())
                : Integer.toUnsignedLong(payload.getInt());

        int consumed = payload.position() - start;
        if (consumed != 2 + tokenLen) {
            System.err.printf("WARN: Length mismatch - claimed %d, consumed %d%n", tokenLen, consumed - 2);
        }

        return new ErrorToken(tokenType, number, state, severity, message, serverName, procName, lineNumber);
    }

    private String readUsVarChar(ByteBuffer buf, int end) {
        int charCount = Short.toUnsignedInt(buf.getShort());

        int byteCount = charCount * 2;  // always 2 bytes per char for UTF-16LE

        byte[] bytes = new byte[byteCount];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_16LE);
    }

    private String readBVarChar(ByteBuffer buf, int end) {
        int charCount = Byte.toUnsignedInt(buf.get());

        int byteCount = charCount * 2;

        byte[] bytes = new byte[byteCount];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.US_ASCII);
    }

}