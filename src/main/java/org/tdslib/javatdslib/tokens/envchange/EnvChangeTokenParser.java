package org.tdslib.javatdslib.tokens.envchange;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;

import java.nio.ByteBuffer;

/**
 * Parser for ENVCHANGE token (0xE3).
 *
 * <p>Eagerly decodes the token and applies the change to the connection context.</p>
 */
public class EnvChangeTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context,
                     final QueryContext queryContext) {
    // Read the total EnvValueData length in bytes (USHORT after the token type 0xE3)
    int envDataLength = payload.getShort() & 0xFFFF;

    // Read the remaining payload bytes for this token (type + values + any binary data)
    byte[] allBytes = new byte[envDataLength];
    payload.get(allBytes);

    // The first byte of allBytes is the change type — extract it
    byte changeTypeByte = allBytes[0];
    EnvChangeType changeType = EnvChangeType.fromByte(changeTypeByte);

    // Keep everything except the type byte for subtype decoding
    byte[] valueBytes = new byte[Math.max(0, envDataLength - 1)];
    if (valueBytes.length > 0) {
      System.arraycopy(allBytes, 1, valueBytes, 0, valueBytes.length);
    }

    return new EnvChangeToken(tokenType, changeType, valueBytes);
  }
}
