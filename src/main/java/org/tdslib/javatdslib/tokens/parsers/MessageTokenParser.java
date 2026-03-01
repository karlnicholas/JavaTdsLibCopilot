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
 * <p>
 * Handles both:
 * <ul>
 * <li>{@link TokenType#ERROR} (0xAA)</li>
 * <li>{@link TokenType#INFO} (0xAB)</li>
 * </ul>
 * <p>
 * These tokens share an identical binary structure but differ in semantic meaning
 * (Errors have severity &gt; 10, Info messages have severity &le; 10).
 */
public class MessageTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context) {

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
    // consumed calculation includes the initial token length field (2 bytes)
    final int consumed = payload.position() - start;
    if (consumed != 2 + tokenLen) {
      final int claimed = tokenLen;
      final int actualConsumed = consumed - 2;
      // In production, you might log this as a warning rather than printing to stderr
      System.err.printf(
              "WARN: Length mismatch in MessageToken - claimed %d, consumed %d%n",
              claimed, actualConsumed
      );
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

  /**
   * Reads a US_VARCHAR (Unsigned Short Length + Unicode Characters).
   */
  private String readUsVarChar(final ByteBuffer buf) {
    // Length is number of CHARACTERS (not bytes)
    final int charCount = Short.toUnsignedInt(buf.getShort());
    if (charCount == 0) {
      return "";
    }

    final int byteCount = charCount * 2; // TDS 7+ uses 2 bytes per char (UTF-16)
    final byte[] bytes = new byte[byteCount];
    buf.get(bytes);

    return new String(bytes, StandardCharsets.UTF_16LE);
  }

  /**
   * Reads a B_VARCHAR (Byte Length + Unicode Characters).
   */
  private String readBvarChar(final ByteBuffer buf) {
    // Length is number of CHARACTERS (not bytes)
    final int charCount = Byte.toUnsignedInt(buf.get());
    if (charCount == 0) {
      return "";
    }

    final int byteCount = charCount * 2; // TDS 7+ uses 2 bytes per char (UTF-16)
    final byte[] bytes = new byte[byteCount];
    buf.get(bytes);

    // CRITICAL FIX: Use UTF_16LE instead of US_ASCII
    return new String(bytes, StandardCharsets.UTF_16LE);
  }
}