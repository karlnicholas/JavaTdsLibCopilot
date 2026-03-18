package org.tdslib.javatdslib.reactive.events;

import org.tdslib.javatdslib.tokens.ColumnData;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.PartialDataColumn;

public record ColumnEvent(ColumnData data) implements TdsStreamEvent {
  @Override public int getByteWeight() {
    if (data instanceof CompleteDataColumn c && c.getData() != null) return c.getData().length;
    if (data instanceof PartialDataColumn p && p.getChunk() != null) return p.getChunk().length;
    return 0;
  }
}
