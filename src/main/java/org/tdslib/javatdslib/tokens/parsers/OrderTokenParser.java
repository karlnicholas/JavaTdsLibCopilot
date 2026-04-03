package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.models.OrderToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.tdslib.javatdslib.tokens.TokenType.ORDER;

/**
 * Parser for ORDER token (0xA9).
 *
 * <p>Decodes the 2-byte length followed by a sequence of 2-byte column indices.</p>
 */
public class OrderTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context) {
    if (tokenType != ORDER.getValue()) {
      final String hex = Integer.toHexString(tokenType & 0xFF);
      throw new IllegalArgumentException(
          "Expected ORDER token (0xA9), got 0x" + hex);
    }

    // 1. Read the length of the token data (UShort)
    final int length = Short.toUnsignedInt(payload.getShort());

    // 2. Read the column indices. Each index is a UShort (2 bytes).
    final int columnCount = length / 2;
    final List<Integer> orderedColumns = new ArrayList<>(columnCount);

    for (int i = 0; i < columnCount; i++) {
      orderedColumns.add(Short.toUnsignedInt(payload.getShort()));
    }

    return new OrderToken(tokenType, orderedColumns);
  }

  @Override
  public boolean canParse(ByteBuffer peekBuffer, ConnectionContext context) {
    if (peekBuffer.remaining() < 2) return false;
    int length = Short.toUnsignedInt(peekBuffer.getShort());
    return peekBuffer.remaining() >= length;
  }
}