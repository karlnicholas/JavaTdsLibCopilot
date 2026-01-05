// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.fedauth;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Parser for FED_AUTH_INFO token (skeleton, not implemented).
 */
public class FedAuthInfoTokenParser extends TokenParser {

    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        // Read token data length (2 bytes)
        int length = tokenStreamHandler.readUInt16LE();

        // Read the token data
        byte[] bytes = tokenStreamHandler.readBytes(length);

        return new FedAuthInfoToken(bytes);
    }
}