package org.tdslib.javatdslib.tokens;

import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;

/**
 * Parser contract for a single token type. Implementations parse one token
 * starting at the current buffer position and return the corresponding object.
 */
public interface TokenParser {

  /**
   * Determines the exact number of bytes required to safely parse this token's payload.
   *
   * @param peekBuffer A read-only duplicate of the current network buffer.
   * @param context    The connection context (needed for version-specific lengths).
   * @return The total payload bytes required, or -1 if the buffer does not contain
   * enough bytes to even read the length header.
   */
  int getRequiredBytes(ByteBuffer peekBuffer, ConnectionContext context);

  /**
   * Parse exactly one token of the given type from the current payload position.
   *
   * @param payload      The ByteBuffer containing the message payload. Position is
   * already right after the token type byte.
   * @param tokenType    The type byte that was just read (for reference/validation).
   * @param context      Access to connection state (for ENV_CHANGE, etc.).
   * @return The parsed token object, or null if no object is needed.
   */
  Token parse(final ByteBuffer payload,
              final byte tokenType,
              final ConnectionContext context);
}