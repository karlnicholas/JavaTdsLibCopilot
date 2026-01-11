package org.tdslib.javatdslib.tokens.done;

import java.nio.ByteBuffer;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Parser for DONE_IN_PROC token (0xFF).
 */
public class DoneInProcTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context,
                     final QueryContext queryContext) {
    if (tokenType != TokenType.DONE_IN_PROC.getValue()) {
      throw new IllegalArgumentException(
          "Expected DONE_IN_PROC token, got 0x" + Integer.toHexString(tokenType & 0xFF)
      );
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

    return new DoneInProcToken(tokenType, status, currentCommand, rowCount);
  }
}