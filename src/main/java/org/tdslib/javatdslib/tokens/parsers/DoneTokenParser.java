package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.protocol.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.models.DoneStatus;
import org.tdslib.javatdslib.tokens.models.DoneToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;

/**
 * Parser for standard DONE token (0xFD).
 */
public class DoneTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context) {
    if (tokenType != TokenType.DONE.getValue()) {
      String hex = Integer.toHexString(tokenType & 0xFF);
      throw new IllegalArgumentException("Expected DONE token, got 0x" + hex);
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

    return new DoneToken(tokenType, status, currentCommand, rowCount);
  }

  @Override
  public int getRequiredBytes(ByteBuffer peekBuffer, ConnectionContext context) {
    // Status (2) + CurCmd (2) + RowCount (8 or 4 depending on TDS version)
    if (context.getTdsVersion().ordinal() >= org.tdslib.javatdslib.protocol.TdsVersion.V7_2.ordinal()) {
      return 12;
    } else {
      return 8;
    }
  }
}
