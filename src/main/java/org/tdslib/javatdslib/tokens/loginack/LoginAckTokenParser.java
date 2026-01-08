package org.tdslib.javatdslib.tokens.loginack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Parser for LOGINACK token (0xAD).
 */
public class LoginAckTokenParser implements TokenParser {
    private static final Logger logger = LoggerFactory.getLogger(LoginAckTokenParser.class);

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context, QueryContext queryContext) {
        if (tokenType != TokenType.LOGIN_ACK.getValue()) {
            throw new IllegalArgumentException("Expected LOGINACK (0xAD), got 0x" + Integer.toHexString(tokenType & 0xFF));
        }

        // Skip total length if not needed (or read it for validation)
        int tokenDataLength = Short.toUnsignedInt(payload.getShort());

        // Interface type
        byte interfaceTypeByte = payload.get();
        SqlInterfaceType interfaceType = SqlInterfaceType.fromByte(interfaceTypeByte);

        // TDS version: 4-byte little-endian DWORD
        int tdsVersionValue = payload.getInt(); // little-endian by default in ByteBuffer
        int bigEndianValue = Integer.reverseBytes(tdsVersionValue); // to match your enum
        TdsVersion tdsVersion = TdsVersion.fromValue(bigEndianValue);

        // Server name: 1 byte char count + UTF-16LE data
        int serverCharLen = payload.get() & 0xFF;
        byte[] serverBytes = new byte[serverCharLen * 2];
        payload.get(serverBytes);
        String serverName = new String(serverBytes, StandardCharsets.UTF_16LE).trim();

        // Server version: 4-byte little-endian int
        int serverVersionValue = payload.getInt();
        bigEndianValue = Integer.reverseBytes(serverVersionValue); // to match your enum
        ServerVersion serverVersion = ServerVersion.fromValue(bigEndianValue);

        return new LoginAckToken(tokenType, interfaceType, tdsVersion, serverName, serverVersion);
    }
}