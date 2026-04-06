package org.tdslib.javatdslib.reactive.events;

import org.tdslib.javatdslib.tokens.Token;

/**
 * Represents a token event in the stream.
 *
 * @param token The underlying token.
 */
public record TokenEvent(Token token) implements TdsStreamEvent {
  @Override
  public int getByteWeight() {
    return 0;
  }
}
