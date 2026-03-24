package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.protocol.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.models.DoneProcToken;
import org.tdslib.javatdslib.tokens.models.DoneStatus;
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

  @Override
  public int getRequiredBytes(ByteBuffer peekBuffer, ConnectionContext context) {
    // 1. Determine the required bytes based on TDS version
    int required = (context.getTdsVersion().ordinal() >= org.tdslib.javatdslib.protocol.TdsVersion.V7_2.ordinal()) ? 12 : 8;

    // 2. CRITICAL: Check if the buffer actually contains the full token payload
    if (peekBuffer.remaining() < required) {
      return -1;
    }

    // 3. Advance the peek buffer (consistency with your other parsers)
    peekBuffer.position(peekBuffer.position() + required);
    return required;
  }
}