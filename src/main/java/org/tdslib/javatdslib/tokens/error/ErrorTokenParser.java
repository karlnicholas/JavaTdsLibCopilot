package org.tdslib.javatdslib.tokens.error;

import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Parser for ERROR tokens from the TDS stream.
 */
public class ErrorTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context,
                     final QueryContext queryContext) {
    if (tokenType != TokenType.ERROR.getValue()) {
      throw new IllegalArgumentException(
          "Wrong token type: 0x" + String.format("%02X", tokenType));
    }

    final int start = payload.position();
    final int tokenLen = Short.toUnsignedInt(payload.getShort());

    if (tokenLen < 11) {
      throw new IllegalStateException("Token too short: " + tokenLen);
    }

    final int end = payload.position() + tokenLen;
    if (end > payload.limit()) {
      throw new IllegalStateException("Token overflows buffer");
    }

    final long number = Integer.toUnsignedLong(payload.getInt());
    final byte state = payload.get();
    final byte severity = payload.get();

    final String message = readUsVarChar(payload, end);
    final String serverName = readBvarChar(payload, end);
    final String procName = readBvarChar(payload, end);

    final long lineNumber;
    if (context != null
        && context.getTdsVersion().ordinal() < TdsVersion.V7_2.ordinal()) {
      lineNumber = Short.toUnsignedInt(payload.getShort());
    } else {
      lineNumber = Integer.toUnsignedLong(payload.getInt());
    }

    final int consumed = payload.position() - start;
    if (consumed != 2 + tokenLen) {
      final int claimed = tokenLen;
      final int actualConsumed = consumed - 2;
      System.err.printf(
          "WARN: Length mismatch - claimed %d, consumed %d%n",
          claimed, actualConsumed
      );
    }

    return new ErrorToken(
        tokenType, number, state, severity, message, serverName, procName, lineNumber);
  }

  private String readUsVarChar(final ByteBuffer buf, final int end) {
    final int charCount = Short.toUnsignedInt(buf.getShort());
    final int byteCount = charCount * 2;  // UTF-16LE: 2 bytes per char

    final byte[] bytes = new byte[byteCount];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_16LE);
  }

  private String readBvarChar(final ByteBuffer buf, final int end) {
    final int charCount = Byte.toUnsignedInt(buf.get());
    final int byteCount = charCount * 2;

    final byte[] bytes = new byte[byteCount];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.US_ASCII);
  }
}
