package org.tdslib.javatdslib.tokens.returnvalue;

import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;

/**
 * Parser for RETURNVALUE token (0xAC).
 *
 * <p>Decodes the parameter name, status flags, type info, and value.</p>
 */
public class ReturnValueTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context,
                     final QueryContext queryContext) {
    if (tokenType != TokenType.RETURN_VALUE.getValue()) {
      final String hex = Integer.toHexString(tokenType & 0xFF);
      throw new IllegalArgumentException(
          "Expected RETURNVALUE token (0xAC), got 0x" + hex);
    }

    // 1. Parameter name length (1 byte) + name (Unicode)
    int nameLen = payload.get() & 0xFF;           // byte count (not characters)
    String paramName = "";
    if (nameLen > 0) {
      byte[] nameBytes = new byte[nameLen];
      payload.get(nameBytes);
      paramName = new String(nameBytes, context.getEffectiveCharset()); // usually UTF-16LE
    }

    // 2. Status flags (1 byte)
    byte statusFlags = payload.get();

    // 3. User type (usually ignored / 0)
    payload.getInt();  // userType – 4 bytes, often 0

    // 4. Flags (1 byte) – e.g. nullable, etc.
    payload.get();     // flags byte

    // 5. TYPE_INFO (variable length – type, max length, precision, scale, etc.)
    // For simplicity we read a basic value here – real impl needs full type parsing
    // This is a placeholder; replace with your actual value decoder
    Object value = readValue(payload, context, queryContext); // ← implement this

    return new ReturnValueToken(tokenType, paramName, statusFlags, value);
  }

  /**
   * Placeholder: read the actual value based on the TYPE_INFO that was just parsed.
   * In a real implementation, you would:
   * - Parse the full TYPE_INFO structure
   * - Use the type byte to decide how many bytes to read
   * - Decode using the appropriate method (getInt, getLong, getString, etc.)
   */
  private Object readValue(ByteBuffer payload, ConnectionContext context, QueryContext queryContext) {
    // Example: assume it's an integer for demo purposes
    // Replace with real type-aware decoding
    return payload.getInt();
  }
}