package org.tdslib.javatdslib.reactive.events;

/**
 * Represents an error event in the stream.
 *
 * @param error The underlying error.
 */
public record ErrorEvent(Throwable error) implements TdsStreamEvent {
  @Override
  public int getByteWeight() {
    return 0;
  }
}
