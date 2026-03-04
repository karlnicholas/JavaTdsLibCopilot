package org.tdslib.javatdslib.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Abstracts the physical network transport.
 * Handles both synchronous (Login/TLS) and asynchronous (Reactive Query) phases.
 */
public interface NetworkConnection extends AutoCloseable {

  void readFullySync(ByteBuffer buffer) throws IOException;

  void writeDirect(ByteBuffer buffer) throws IOException;

  void enterAsyncMode(int bufferSize) throws IOException;

  void writeAsync(ByteBuffer buffer);

  void setHandlers(Consumer<ByteBuffer> onDataAvailable, Consumer<Throwable> onError);

  /** Suspends reading from the network socket (TCP Backpressure). */
  void suspendRead();

  /** Resumes reading from the network socket. */
  void resumeRead();

  @Override
  void close() throws IOException;
}