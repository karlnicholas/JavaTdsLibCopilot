package org.tdslib.javatdslib.tokens.returnvalue;

import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsType;
import org.tdslib.javatdslib.tokens.*;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;

public class ReturnValueTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context,
                     final QueryContext queryContext) {
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

    byte[] value = DataParser.getDataBytes(payload, typeInfo.getTdsType(), typeInfo.getMaxLength());

    // 6. Value (Decode using TypeInfo)
//    int len = Byte.toUnsignedInt(payload.get());
//    byte[] value = new byte[len];
//    payload.get(value);

    return new ReturnValueToken(tokenType, ordinal, paramName, statusFlags, typeInfo, value);
  }

}