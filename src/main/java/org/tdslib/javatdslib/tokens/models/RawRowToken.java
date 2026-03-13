package org.tdslib.javatdslib.tokens.models;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

import java.nio.ByteBuffer;

public class RawRowToken extends Token {
  private final ByteBuffer payload;

  /**
   * Constructs a new RawRowToken with a defensive copy of the payload.
   *
   * @param type The byte value representing the token type.
   * @param source The raw byte buffer from the network layer.
   */
  public RawRowToken(byte type, ByteBuffer source) {
    super(TokenType.ROW);

    // 1. Allocate a new buffer exactly the size of the current row data
    // This detaches the token from the network thread's shared buffer.
    this.payload = ByteBuffer.allocate(source.remaining());

    // 2. Copy the bytes from the source slice into our new private buffer
    this.payload.put(source);

    // 3. Flip the buffer so it's ready for the mapper (Worker Thread) to read
    this.payload.flip();

    // 4. Set the byte order to match the TDS protocol (Little Endian)
    this.payload.order(source.order());
  }

  public ByteBuffer getPayload() {
    return payload;
  }
}