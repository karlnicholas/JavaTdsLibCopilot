// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.envchange;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.TokenStreamHandler;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Environment change token parser.
 */
public class EnvChangeTokenParser extends TokenParser {

    @Override
    public Token parse(TokenType tokenType, TokenStreamHandler handler) {
        // Read Length (2 bytes)
        int length = handler.readUInt16LE();

        // Read SubType (1 byte)
        int subTypeValue = handler.readUInt8();
        EnvChangeTokenSubType subType = EnvChangeTokenSubType.fromValue((byte) subTypeValue);

        if (subType == null) {
            throw new IllegalArgumentException("Unknown EnvChange sub type: " + subTypeValue);
        }

        if (subType == EnvChangeTokenSubType.PACKET_SIZE) {
            // Packet size is sent as two 4-byte little-endian integers (new, old)
            // Note: readUInt32LE returns long, we cast to int as packet sizes fit in int
            int newPacketSize = (int) handler.readUInt32LE();
            int oldPacketSize = (int) handler.readUInt32LE();

            // Update connection options packet size
            try {
                handler.getOptions().setPacketSize(newPacketSize);
            } catch (IllegalArgumentException ex) {
                // ignore invalid packet size from server
            }

            return new EnvChangeToken(
                    subType,
                    String.valueOf(oldPacketSize),
                    String.valueOf(newPacketSize)
            );
        } else {
            // Standard string-based EnvChanges (Database, Language, Charset, Routing)
            String newValue = handler.readBVarChar();
            String oldValue = handler.readBVarChar();

            if (subType == EnvChangeTokenSubType.ROUTING) {
                try {
                    handler.getOptions().setRoutingHint(newValue);
                } catch (Exception ex) {
                    // ignore failures when storing routing hint
                }
            }

            return new EnvChangeToken(subType, oldValue, newValue);
        }
    }
}