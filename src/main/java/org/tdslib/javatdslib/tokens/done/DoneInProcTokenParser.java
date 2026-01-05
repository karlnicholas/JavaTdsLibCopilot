package org.tdslib.javatdslib.tokens.done;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

import java.nio.ByteBuffer;

/**
 * Parser for DONE_IN_PROC token (0xFF).
 */
public class DoneInProcTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context) {
        if (tokenType != TokenType.DONE_IN_PROC.getValue()) {
            throw new IllegalArgumentException("Expected DONE_IN_PROC token, got 0x" + Integer.toHexString(tokenType & 0xFF));
        }

        int statusValue = Short.toUnsignedInt(payload.getShort());
        DoneStatus status = DoneStatus.fromValue(statusValue);

        int currentCommand = Short.toUnsignedInt(payload.getShort());

        long rowCount;
        if (context.getTdsVersion().ordinal() >= TdsVersion.V7_2.ordinal()) {
            rowCount = payload.getLong();
        } else {
            rowCount = Integer.toUnsignedLong(payload.getInt());
        }

        return new DoneInProcToken(status, currentCommand, rowCount);
    }
}