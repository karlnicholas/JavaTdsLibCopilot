package org.tdslib.javatdslib.reactive.events;

public record ErrorEvent(Throwable error) implements TdsStreamEvent {
  @Override public int getByteWeight() { return 0; }
}
