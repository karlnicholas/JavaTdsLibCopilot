// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.loginack;

import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Login ack token parser.
 */
public class LoginAckTokenParser extends TokenParser {

    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler handler) {
        // Read Length (2 bytes)
        // This is the total length of the data that follows. We read it to advance the stream
        // but generally parse the specific fields individually below.
        int length = handler.readUInt16LE();

        // Read Interface Type (1 byte)
        int type = handler.readUInt8();
        SqlInterfaceType interfaceType = SqlInterfaceType.fromValue((byte) type);
        if (interfaceType == null) {
            throw new IllegalArgumentException("Unknown Sql Interface type: " + type);
        }

        // Read TDS Version (4 bytes, Big Endian)
        long version = handler.readUInt32BE();
        TdsVersion tdsVersion = TdsVersion.fromValue((int) version);
        if (tdsVersion == null) {
            throw new IllegalArgumentException("Unknown Tds Version: " + Integer.toHexString((int) version));
        }

        // Read Program Name (BVarChar)
        String progName = handler.readBVarChar();

        // Read Program Version (4 bytes total: Major, Minor, BuildHi, BuildLow)
        int major = handler.readUInt8();
        int minor = handler.readUInt8();
        int buildHi = handler.readUInt8();
        int buildLow = handler.readUInt8();

        ProgVersion progVersion = new ProgVersion(
                (byte) major,
                (byte) minor,
                (byte) buildHi,
                (byte) buildLow
        );

        return new LoginAckToken(interfaceType, tdsVersion, progName, progVersion);
    }
}