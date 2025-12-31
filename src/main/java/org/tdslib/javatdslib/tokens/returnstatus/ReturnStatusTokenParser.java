// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.returnstatus;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Parser for RETURN_STATUS token.
 */
public class ReturnStatusTokenParser extends TokenParser {

    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler tokenStreamHandler) {
        long value = tokenStreamHandler.readUInt32LE();
        return new ReturnStatusToken((int) value);
    }
}