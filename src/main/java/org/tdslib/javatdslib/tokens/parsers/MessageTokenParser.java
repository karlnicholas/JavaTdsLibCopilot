package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.protocol.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.models.ErrorToken;
import org.tdslib.javatdslib.tokens.models.InfoToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A unified parser for TDS "Message" tokens.
 */
public class MessageTokenParser implements TokenParser {

  @Override
  public Token parse(
      final ByteBuffer payload, final byte tokenType, final ConnectionContext context) {

    // 1. Validate that this parser is handling the correct token types
    if (tokenType != TokenType.ERROR.getValue() && tokenType != TokenType.INFO.getValue()) {
      throw new IllegalArgumentException(
          "MessageTokenParser expects ERROR (0xAA) or INFO (0xAB), but got: 0x"
              + String.format("%02X", tokenType));
    }

    final int start = payload.position();

    // 2. Read the total length of the token (unsigned short)
    final int tokenLen = Short.toUnsignedInt(payload.getShort());

    // Basic sanity check: Minimum headers take ~11 bytes
    if (tokenLen < 11) {
      throw new IllegalStateException("Token too short: " + tokenLen);
    }

    final int end = payload.position() + tokenLen;
    if (end > payload.limit()) {
      throw new IllegalStateException("Token length overflows buffer");
    }

    // 3. Parse Fixed Header Fields
    // Number: 4 bytes (Long)
    final long number = Integer.toUnsignedLong(payload.getInt());
    // State: 1 byte
    final byte state = payload.get();
    // Severity: 1 byte
    final byte severity = payload.get();

    // 4. Parse Variable Length Strings
    // Message Text (US_VARCHAR)
    final String message = readUsVarChar(payload);
    // Server Name (B_VARCHAR)
    final String serverName = readBvarChar(payload);
    // Proc Name (B_VARCHAR)
    final String procName = readBvarChar(payload);

    // 5. Parse Line Number (Version Dependent)
    final long lineNumber;
    if (context != null && context.getTdsVersion().ordinal() < TdsVersion.V7_2.ordinal()) {
      // Pre-7.2 uses 2 bytes for line number
      lineNumber = Short.toUnsignedInt(payload.getShort());
    } else {
      // 7.2+ uses 4 bytes for line number
      lineNumber = Integer.toUnsignedLong(payload.getInt());
    }

    // 6. Verify Consumption
    final int consumed = payload.position() - start;
    if (consumed != 2 + tokenLen) {
      final int claimed = tokenLen;
      final int actualConsumed = consumed - 2;
      System.err.printf(
          "WARN: Length mismatch in MessageToken - claimed %d, consumed %d%n",
          claimed, actualConsumed);
    }

    // 7. Return specific Token implementation
    if (tokenType == TokenType.INFO.getValue()) {
      return new InfoToken(
          tokenType, number, state, severity, message, serverName, procName, lineNumber);
    } else {
      return new ErrorToken(
          tokenType, number, state, severity, message, serverName, procName, lineNumber);
    }
  }

  private String readUsVarChar(final ByteBuffer buf) {
    final int charCount = Short.toUnsignedInt(buf.getShort());
    if (charCount == 0) {
      return "";
    }
    final int byteCount = charCount * 2;
    final byte[] bytes = new byte[byteCount];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_16LE);
  }

  private String readBvarChar(final ByteBuffer buf) {
    final int charCount = Byte.toUnsignedInt(buf.get());
    if (charCount == 0) {
      return "";
    }
    final int byteCount = charCount * 2;
    final byte[] bytes = new byte[byteCount];
    buf.get(bytes);
    return new String(bytes, StandardCharsets.UTF_16LE);
  }

  @Override
  public boolean canParse(ByteBuffer peekBuffer, ConnectionContext context) {
    if (peekBuffer.remaining() < 2) {
      return false;
    }
    int tokenLen = Short.toUnsignedInt(peekBuffer.getShort());
    return peekBuffer.remaining() >= tokenLen;
  }
}
