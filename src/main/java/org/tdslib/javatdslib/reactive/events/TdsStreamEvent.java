package org.tdslib.javatdslib.reactive.events;

/**
 * An event from the TDS token stream.
 */
public sealed interface TdsStreamEvent permits TokenEvent, ColumnEvent, ErrorEvent {
  /**
   * The approximate byte weight of this event in the queue.
   *
   * @return byte weight.
   */
  int getByteWeight();
}
