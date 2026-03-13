package org.tdslib.javatdslib.reactive;

/**
 * A pure Java callback interface used by the drainer to push data.
 * No Reactive Streams dependencies.
 */
public interface DataSink<T> {
  void pushNext(T item);
  void pushComplete();
  void pushError(Throwable t);
}