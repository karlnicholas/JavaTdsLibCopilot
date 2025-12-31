// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.done;

import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * DoneInProc token parser.
 */
public class DoneInProcTokenParser extends TokenParser {

    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler handler) {
        // Read Status (2 bytes)
        int statusValue = handler.readUInt16LE();
        DoneStatus doneStatus = DoneStatus.fromValue(statusValue);

        // Read Current Command (2 bytes)
        int currentCommand = handler.readUInt16LE();

        // Read Row Count (size depends on TDS version)
        long rowCount;
        if (handler.getOptions().getTdsVersion().ordinal() > TdsVersion.V7_2.ordinal()) {
            rowCount = handler.readUInt64LE();
        } else {
            rowCount = handler.readUInt32LE();
        }

        return new DoneInProcToken(doneStatus, currentCommand, rowCount);
    }
}