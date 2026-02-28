package org.tdslib.javatdslib.tokens.done;

import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;

/**
 * Parser for DONE_PROC token (0xFE).
 */
public class DoneProcTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context) {
    if (tokenType != TokenType.DONE_PROC.getValue()) {
      throw new IllegalArgumentException(
          "Expected DONE_PROC token, got 0x" + Integer.toHexString(tokenType & 0xFF)
      );
    }

    int statusValue = Short.toUnsignedInt(payload.getShort());
    DoneStatus status = new DoneStatus(statusValue);

    int currentCommand = Short.toUnsignedInt(payload.getShort());

    long rowCount;
    if (context.getTdsVersion().ordinal() >= TdsVersion.V7_2.ordinal()) {
      rowCount = payload.getLong();
    } else {
      rowCount = Integer.toUnsignedLong(payload.getInt());
    }

    return new DoneProcToken(tokenType, status, currentCommand, rowCount);
  }
}