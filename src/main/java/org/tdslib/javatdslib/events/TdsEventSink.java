package org.tdslib.javatdslib.events;

/**
 * A highly granular sink for receiving fully assembled logical events
 * from the TDS protocol stream.
 */
public interface TdsEventSink {
  void onRowStart();

  void onCompleteColumn(int columnIndex, CompleteDataColumn column);

  void onPartialColumnChunk(int columnIndex, PartialDataColumn chunk);

  void onRowEnd();

  void onUpdateCount(long count);

  void onDone();

  void onError(Throwable t);
}