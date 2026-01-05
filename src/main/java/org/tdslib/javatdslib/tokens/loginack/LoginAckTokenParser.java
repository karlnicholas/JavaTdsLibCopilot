package org.tdslib.javatdslib.tokens.loginack;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Parser for LOGINACK token (0xAD).
 */
public class LoginAckTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context) {
        // Interface type (0x01 = SQL_DFLT, 0x02 = SQL_TSQL)
        byte interfaceTypeByte = payload.get();
        SqlInterfaceType interfaceType = SqlInterfaceType.fromByte(interfaceTypeByte);

        // TDS version (major.minor.build)
        byte major = payload.get();
        byte minor = payload.get();
        short build = payload.getShort();

        TdsVersion tdsVersion = new TdsVersion(major, minor, build);

        // Program name length (1 byte) + name (ASCII)
        int progNameLen = payload.get() & 0xFF;
        byte[] progNameBytes = new byte[progNameLen];
        payload.get(progNameBytes);
        String progName = new String(progNameBytes, StandardCharsets.US_ASCII);

        // Program version (4 bytes: major, minor, build, sub-build)
        byte progMajor = payload.get();
        byte progMinor = payload.get();
        short progBuild = payload.getShort();
        byte progSubBuild = payload.get();
        ProgVersion progVersion = new ProgVersion(progMajor, progMinor, progBuild, progSubBuild);

        return new LoginAckToken(
                interfaceType,
                tdsVersion,
                progName,
                progVersion
        );
    }
}