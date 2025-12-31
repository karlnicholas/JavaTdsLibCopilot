// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.returnvalue;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Parser for RETURN_VALUE token (not yet implemented).
 */
public class ReturnValueTokenParser extends TokenParser {

    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        String name = tokenStreamHandler.readUsVarChar();

        // Note: This logic assumes the next 4 bytes are the length of the data.
        // In a full TDS implementation, you would typically parse Status, UserType, and TypeInfo here.
        long length = tokenStreamHandler.readUInt32LE();

        byte[] bytes = tokenStreamHandler.readBytes((int) length);

        return new ReturnValueToken(name, bytes);
    }
}