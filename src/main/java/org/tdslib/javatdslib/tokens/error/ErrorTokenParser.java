// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.error;

import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Error token parser.
 */
public class ErrorTokenParser extends TokenParser {

    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler handler) {
        // Read Length (2 bytes)
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

        return new ErrorToken(
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