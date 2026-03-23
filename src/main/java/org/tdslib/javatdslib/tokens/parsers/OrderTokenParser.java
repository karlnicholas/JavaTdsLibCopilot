package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.models.OrderToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for ORDER token (0xA9).
 *
 * <p>Decodes the 2-byte length followed by a sequence of 2-byte column indices.</p>
 */
public class OrderTokenParser implements TokenParser {

  private static final byte ORDER_TOKEN_VALUE = (byte) 0xA9;

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context) {
    if (tokenType != ORDER_TOKEN_VALUE) {
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
  public int getRequiredBytes(ByteBuffer peekBuffer, ConnectionContext context) {
    int startPos = peekBuffer.position();

    // 1. We need at least 2 bytes to read the length header
    if (peekBuffer.remaining() < 2) {
      return -1;
    }

    // Read the claimed length (unsigned short)
    int length = Short.toUnsignedInt(peekBuffer.getShort());

    // 2. Check if the buffer has enough bytes for all the column indices
    if (peekBuffer.remaining() < length) {
      return -1;
    }

    // Advance the buffer past this token's data
    peekBuffer.position(peekBuffer.position() + length);

    // Total bytes = 2 (header) + length (column indices data)
    return peekBuffer.position() - startPos;
  }
}