package org.tdslib.javatdslib.reactive.events;

public sealed interface TdsStreamEvent permits TokenEvent, ColumnEvent, ErrorEvent {
  int getByteWeight();
}

