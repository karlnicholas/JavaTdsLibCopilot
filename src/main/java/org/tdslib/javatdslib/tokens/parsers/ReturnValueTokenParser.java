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
    short ordinal = payload.getShort();
    int nameLen = payload.get() & 0xFF;
    String paramName = "";
    if (nameLen > 0) {
      byte[] nameBytes = new byte[nameLen * 2];
      payload.get(nameBytes);
      paramName = new String(nameBytes, context.getEffectiveCharset());
    }

    // 2. Status flags
    byte statusFlags = payload.get();

    // 3. User type
    int userType = payload.getInt();

    // 4. Flags
    short flags = payload.getShort();

    // 5. TYPE_INFO (Robust Parsing)
    TypeInfo typeInfo = TypeInfoParser.parse(payload);

    // FIX: Pass null for streaming args, and provide the context's charset
    byte[] value =
        (byte[])
            DataParser.getDataBytes(
                payload,
                typeInfo.getTdsType(),
                typeInfo.getMaxLength(),
                context.getEffectiveCharset());

    return new ReturnValueToken(tokenType, ordinal, paramName, statusFlags, typeInfo, value);
  }

  @Override
  public int getRequiredBytes(ByteBuffer peekBuffer, ConnectionContext context) {
    int startPos = peekBuffer.position();

    // 1. Ordinal (2 bytes) + Param Name Length (1 byte)
    if (peekBuffer.remaining() < 3) return -1;
    peekBuffer.position(peekBuffer.position() + 2); // skip ordinal
    int nameLen = peekBuffer.get() & 0xFF;

    // 2. Param Name Bytes (nameLen * 2 characters)
    int nameBytesLen = nameLen * 2;
    if (peekBuffer.remaining() < nameBytesLen) return -1;
    peekBuffer.position(peekBuffer.position() + nameBytesLen);

    // 3. Status Flags (1 byte) + User Type (4 bytes) + Flags (2 bytes) = 7 bytes
    if (peekBuffer.remaining() < 7) return -1;
    peekBuffer.position(peekBuffer.position() + 7);

    // 4. Safely verify and parse the TYPE_INFO
    int typeInfoStart = peekBuffer.position();
    if (TypeInfoParser.getRequiredBytes(peekBuffer) == -1) {
      return -1; // Incomplete TypeInfo
    }

    // Since getRequiredBytes advanced the position, we must rewind it to actually parse the TypeInfo
    // object so we know how to calculate the length of the data value.
    peekBuffer.position(typeInfoStart);
    TypeInfo typeInfo = TypeInfoParser.parse(peekBuffer);

    // 5. Calculate Required Bytes for Data Value
    // We delegate this to a new safe method in DataParser
    int dataBytesRequired = DataParser.getRequiredValueBytes(peekBuffer, typeInfo.getTdsType(), typeInfo.getMaxLength());
    if (dataBytesRequired == -1) {
      return -1;
    }

    peekBuffer.position(peekBuffer.position() + dataBytesRequired);

    return peekBuffer.position() - startPos;
  }
}
