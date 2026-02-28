package org.tdslib.javatdslib.tokens.loginack;

import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Parser for LOGINACK token (0xAD).
 */
public class LoginAckTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context) {
    if (tokenType != TokenType.LOGIN_ACK.getValue()) {
      final String hex = Integer.toHexString(tokenType & 0xFF);
      throw new IllegalArgumentException("Expected LOGINACK (0xAD), got 0x" + hex);
    }

    // Skip total length if not needed (or read it for validation)
    Short.toUnsignedInt(payload.getShort());

    // Interface type
    final byte interfaceTypeByte = payload.get();
    final SqlInterfaceType interfaceType = SqlInterfaceType.fromByte(interfaceTypeByte);

    // TDS version: 4-byte little-endian DWORD
    final int tdsVersionValue = payload.getInt(); // little-endian by default in ByteBuffer
    final int bigEndianValue = Integer.reverseBytes(tdsVersionValue); // to match your enum
    final TdsVersion tdsVersion = TdsVersion.fromValue(bigEndianValue);

    // Server name: 1 byte char count + UTF-16LE data
    final int serverCharLen = payload.get() & 0xFF;
    final byte[] serverBytes = new byte[serverCharLen * 2];
    payload.get(serverBytes);
    final String serverName = new String(serverBytes, StandardCharsets.UTF_16LE).trim();

    // Server version: 4-byte little-endian int
    final int serverVersionValue = payload.getInt();
    final int serverBigEndian = Integer.reverseBytes(serverVersionValue); // to match your enum
    final ServerVersion serverVersion = ServerVersion.fromValue(serverBigEndian);

    return new LoginAckToken(tokenType, interfaceType, tdsVersion, serverName, serverVersion);
  }
}
