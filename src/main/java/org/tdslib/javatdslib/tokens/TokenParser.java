// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens;

import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;

/**
 * Parser contract for a single token type. Implementations parse one token
 * starting at the current buffer position and return the corresponding object.
 */
public interface TokenParser {

  /**
   * Parse exactly one token of the given type from the current payload position.
   *
   * @param payload      The ByteBuffer containing the message payload. Position is
   *                     already right after the token type byte.
   * @param tokenType    The type byte that was just read (for reference/validation).
   * @param context      Access to connection state (for ENV_CHANGE, etc.).
   * @param queryContext Query-specific context storage.
   * @return The parsed token object, or null if no object is needed.
   */
  Token parse(final ByteBuffer payload,
              final byte tokenType,
              final ConnectionContext context,
              final QueryContext queryContext);
}