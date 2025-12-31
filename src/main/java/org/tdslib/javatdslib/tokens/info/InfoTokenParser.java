// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.info;

import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Info token parser.
 */
public class InfoTokenParser extends TokenParser {

    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler handler) {
        // Read token length (2 bytes)
        // Note: The length covers the rest of the token, but we parse field-by-field.
        int length = handler.readUInt16LE();

        long number = handler.readUInt32LE();
        int state = handler.readUInt8();
        int severity = handler.readUInt8();
        String message = handler.readUsVarChar();
        String serverName = handler.readBVarChar();
        String procName = handler.readBVarChar();

        long lineNumber;
        if (handler.getOptions().getTdsVersion().ordinal() < TdsVersion.V7_2.ordinal()) {
            lineNumber = handler.readUInt16LE();
        } else {
            lineNumber = handler.readUInt32LE();
        }

        return new InfoToken(
                number,
                (byte) state,
                (byte) severity,
                message,
                serverName,
                procName,
                lineNumber
        );
    }
}