// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;

import java.nio.ByteBuffer;

public interface TokenParser {

    /**
     * Parse exactly one token of the given type from the current payload position.
     *
     * @param payload     The ByteBuffer containing the message payload.
     *                    Position is already right after the token type byte.
     * @param tokenType   The type byte that was just read (for reference/validation)
     * @param context     Access to connection state (for ENV_CHANGE, etc.)
     * @return            The parsed token object, or null if no object is needed
     */
    Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context, QueryContext queryContext);
}