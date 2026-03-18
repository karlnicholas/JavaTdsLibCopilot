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
}
