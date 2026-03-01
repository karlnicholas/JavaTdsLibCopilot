package org.tdslib.javatdslib.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

/**
 * Abstracts the physical network transport.
 * Handles both synchronous (Login/TLS) and asynchronous (Reactive Query) phases.
 */
public interface NetworkConnection extends AutoCloseable {

  /** Synchronously reads data until the buffer is full. */
  void readFullySync(ByteBuffer buffer) throws IOException;

  /** Synchronously writes the entire buffer to the network. */
  void writeDirect(ByteBuffer buffer) throws IOException;

  /** Transitions the connection to non-blocking mode and starts the event loop. */
  void enterAsyncMode(int bufferSize) throws IOException;

  /** Queues a buffer for asynchronous writing. */
  void writeAsync(ByteBuffer buffer);

  /** * Sets the callbacks for asynchronous events.
   * @param onDataAvailable Called when new bytes are read into the buffer.
   * @param onError Called when a network or event loop error occurs.
   */
  void setHandlers(Consumer<ByteBuffer> onDataAvailable, Consumer<Throwable> onError);

  /** Exposes the underlying channel (useful for the existing TLS Handshake implementation). */
  SocketChannel getSocketChannel();

  /** * Overrides AutoCloseable to restrict the thrown exception to IOException
   */
  @Override
  void close() throws IOException;
}