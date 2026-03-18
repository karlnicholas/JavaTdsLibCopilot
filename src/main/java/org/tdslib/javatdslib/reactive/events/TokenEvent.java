package org.tdslib.javatdslib.reactive.events;

import org.tdslib.javatdslib.tokens.Token;

public record TokenEvent(Token token) implements TdsStreamEvent {
  @Override public int getByteWeight() { return 0; }
}
