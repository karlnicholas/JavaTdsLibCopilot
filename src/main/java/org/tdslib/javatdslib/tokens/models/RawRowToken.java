package org.tdslib.javatdslib.tokens.models;

import java.nio.ByteBuffer;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Represents a raw row token in the TDS protocol. This token contains the raw byte data for a
 * single row, which needs to be parsed based on the column metadata.
 */
public class RawRowToken extends Token {
  private final ByteBuffer payload;

  /**
   * Constructs a new RawRowToken.
   *
   * @param type The byte value representing the token type (unused, as TokenType is fixed).
   * @param payload The raw byte buffer containing the row data.
   */
  public RawRowToken(byte type, ByteBuffer payload) {
    // 1. Initialize the parent Token class with the correct TokenType
    super(TokenType.ROW);

    // We don't need to store the byte 'type' internally anymore
    // since the parent class handles the TokenType.
    this.payload = payload;
  }

  public ByteBuffer getPayload() {
    return payload;
  }

  // 2. Removed the overridden getType() method
}
