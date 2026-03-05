package org.tdslib.javatdslib.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Abstracts the physical network transport. Handles both synchronous (Login/TLS) and asynchronous
 * (Reactive Query) phases.
 */
public interface NetworkConnection extends AutoCloseable {

  /**
   * Reads data synchronously from the network into the provided buffer until it is full.
   *
   * @param buffer The buffer to read data into.
   * @throws IOException If an I/O error occurs.
   */
  void readFullySync(ByteBuffer buffer) throws IOException;

  /**
   * Writes data synchronously to the network from the provided buffer.
   *
   * @param buffer The buffer containing data to write.
   * @throws IOException If an I/O error occurs.
   */
  void writeDirect(ByteBuffer buffer) throws IOException;

  /**
   * Transitions the connection to asynchronous mode.
   *
   * @param bufferSize The size of the read buffer to use in async mode.
   * @throws IOException If an I/O error occurs during the transition.
   */
  void enterAsyncMode(int bufferSize) throws IOException;

  /**
   * Writes data asynchronously to the network.
   *
   * @param buffer The buffer containing data to write.
   */
  void writeAsync(ByteBuffer buffer);

  /**
   * Sets the handlers for asynchronous data and error events.
   *
   * @param onDataAvailable The consumer to invoke when data is available.
   * @param onError The consumer to invoke when an error occurs.
   */
  void setHandlers(Consumer<ByteBuffer> onDataAvailable, Consumer<Throwable> onError);

  /** Suspends reading from the network socket (TCP Backpressure). */
  void suspendRead();

  /** Resumes reading from the network socket. */
  void resumeRead();

  @Override
  void close() throws IOException;
}
