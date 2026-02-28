package org.tdslib.javatdslib.tokens.row;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

import java.nio.ByteBuffer;

public class RawRowToken extends Token {
  private final ByteBuffer payload;

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