package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.models.ReturnValueToken;
import org.tdslib.javatdslib.tokens.models.TypeInfo;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;

/**
 * Parser for the RETURNVALUE token (0xAC). This token is used to return output parameters from
 * stored procedures or user-defined functions.
 */
public class ReturnValueTokenParser implements TokenParser {

  @Override
  public Token parse(
      final ByteBuffer payload, final byte tokenType, final ConnectionContext context) {
    if (tokenType != TokenType.RETURN_VALUE.getValue()) {
      throw new IllegalArgumentException("Expected RETURNVALUE token (0xAC)");
    }

    // 1. Ordinal & Parameter Name
    final short ordinal = payload.getShort();
    int nameLen = payload.get() & 0xFF;
    String paramName = "";
    if (nameLen > 0) {
      byte[] nameBytes = new byte[nameLen * 2];
      payload.get(nameBytes);
      paramName = new String(nameBytes, context.getEffectiveCharset());
    }

    // 2. Status Flags (1 byte) + User Type (4 bytes) + Flags (2 bytes)
    byte statusFlags = payload.get();
    payload.getInt(); // Skip UserType
    payload.getShort(); // Skip Flags

    // 3. TYPE_INFO (Parsed, but data is NOT extracted here)
    TypeInfo typeInfo = TypeInfoParser.parse(payload);

    // Return a token with a null value; StatefulTokenDecoder will fill the data
    return new ReturnValueToken(tokenType, ordinal, paramName, statusFlags, typeInfo, null);
  }

  @Override
  public boolean canParse(ByteBuffer peekBuffer, ConnectionContext context) {
    int startPos = peekBuffer.position();
    try {
      // 1. Ordinal (2 bytes) + Param Name Length (1 byte)
      if (peekBuffer.remaining() < 3) {
        return false;
      }
      peekBuffer.getShort(); // skip ordinal
      int nameLen = peekBuffer.get() & 0xFF;

      // 2. Param Name Bytes (Unicode = nameLen * 2)
      int nameBytesLen = nameLen * 2;
      if (peekBuffer.remaining() < nameBytesLen) {
        return false;
      }
      peekBuffer.position(peekBuffer.position() + nameBytesLen);

      // 3. Status Flags (1) + User Type (4) + Flags (2) = 7 bytes
      if (peekBuffer.remaining() < 7) {
        return false;
      }
      peekBuffer.position(peekBuffer.position() + 7);

      // 4. Safely verify TypeInfo
      return TypeInfoParser.canParse(peekBuffer);
    } catch (Exception e) {
      return false;
    } finally {
      // CRITICAL: Always rewind so parse() starts at the Ordinal
      peekBuffer.position(startPos);
    }
  }
}
