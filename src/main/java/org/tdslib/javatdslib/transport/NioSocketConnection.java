package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * An implementation of {@link NetworkConnection} using Java NIO {@link SocketChannel}. This class
 * handles both synchronous and asynchronous I/O operations, managing a background event loop for
 * non-blocking communication.
 */
public class NioSocketConnection implements NetworkConnection {
  private static final Logger logger = LoggerFactory.getLogger(NioSocketConnection.class);
  private ExecutorService eventLoopExecutor;

  private final SocketChannel socketChannel;
  private Selector selector;
  private ByteBuffer readBuffer;

  private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean pendingWrite = new AtomicBoolean(false);

  private Consumer<ByteBuffer> onDataAvailable;
  private Consumer<Throwable> onError;

  /**
   * Constructs a new NioSocketConnection.
   *
   * @param host          The hostname to connect to.
   * @param port          The port to connect to.
   * @param readTimeoutMs The read timeout in milliseconds for synchronous operations.
   * @throws IOException If an I/O error occurs during connection establishment.
   */
  public NioSocketConnection(String host, int port, int readTimeoutMs) throws IOException {
    this.socketChannel = SocketChannel.open();
    this.socketChannel.configureBlocking(true);
    this.socketChannel.socket().setSoTimeout(readTimeoutMs);

    logger.debug(
        "Initiating physical TCP connection to {}:{} with timeout {}ms",
        host, port, readTimeoutMs);
    InetSocketAddress address = new InetSocketAddress(host, port);

    // FIX: Enforce a strict connection timeout instead of infinite block
    this.socketChannel.socket().connect(address, readTimeoutMs);
  }

  @Override
  public void readFullySync(ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      int read = socketChannel.read(buffer);
      if (read == -1) {
        throw new IOException("EOF during sync read");
      }
    }
  }

  @Override
  public void writeDirect(ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      socketChannel.write(buffer);
    }
  }

  @Override
  public void enterAsyncMode(int bufferSize) throws IOException {
    logger.debug(
        "Entering asynchronous mode. Starting NIO event loop (Buffer: {} bytes)", bufferSize);
    this.readBuffer = ByteBuffer.allocate(bufferSize);
    this.selector = Selector.open();
    this.socketChannel.configureBlocking(false);
    this.socketChannel.register(selector, SelectionKey.OP_READ, this);
    startEventLoop();
  }

  @Override
  public void writeAsync(ByteBuffer buffer) {
    if (!socketChannel.isOpen()) {
      throw new IllegalStateException("Cannot write: Socket is closed");
    }

    writeQueue.offer(buffer.duplicate());

    // We don't care if it was empty. If we successfully transition pendingWrite
    // from false to true, WE are responsible for waking the selector.
    if (pendingWrite.compareAndSet(false, true)) {
      SelectionKey key = socketChannel.keyFor(selector);
      if (key != null && key.isValid()) {
        key.interestOpsOr(SelectionKey.OP_WRITE);
        selector.wakeup();
      } else {
        throw new IllegalStateException("Cannot write: SelectionKey is invalid (socket closed)");
      }
    }
  }

  private void onWritable(SelectionKey key) throws IOException {
    while (true) {
      ByteBuffer buf = writeQueue.peek();
      if (buf == null) {
        // 1. Turn off the write interest and the flag
        key.interestOpsAnd(~SelectionKey.OP_WRITE);
        pendingWrite.set(false);

        // 2. THE CRITICAL DOUBLE-CHECK
        // If a thread sneaked a buffer into the queue while we were turning off the flag,
        // we must turn it back on and continue the loop to prevent deadlock.
        if (!writeQueue.isEmpty() && pendingWrite.compareAndSet(false, true)) {
          key.interestOpsOr(SelectionKey.OP_WRITE);
          continue;
        }
        return;
      }

      int written = socketChannel.write(buf);

      // If OS buffer is full (written == 0) or we only wrote part of the buffer,
      // leave pendingWrite=true and OP_WRITE active. We will resume when OS signals us.
      if (buf.hasRemaining()) {
        return;
      }
      writeQueue.poll();
    }
  }

  @Override
  public void setHandlers(Consumer<ByteBuffer> onDataAvailable, Consumer<Throwable> onError) {
    this.onDataAvailable = onDataAvailable;
    this.onError = onError;
  }

  private void startEventLoop() {
    // Use an ExecutorService with a custom ThreadFactory for clean naming and daemon status
    this.eventLoopExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "TDS-EventLoop-" + socketChannel.socket().getLocalPort());
              t.setDaemon(true);
              return t;
            });

    eventLoopExecutor.submit(
        () -> {
          while (!Thread.currentThread().isInterrupted()) {
            try {
              if (selector.select(1000) == 0) {
                continue;
              }
              Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
              while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();
                if (!key.isValid()) {
                  continue;
                }
                try {
                  if (key.isReadable()) {
                    onReadable(key);
                  }
                  if (key.isWritable()) {
                    onWritable(key);
                  }
                } catch (Throwable t) {
                  cleanupKeyAndTransport(key);
                  if (onError != null) {
                    onError.accept(t);
                  }
                }
              }
            } catch (Throwable fatal) {
              if (onError != null) {
                onError.accept(fatal);
              }
              break; // Exit the event loop
            }
          }
        });
  }

  private void onReadable(SelectionKey selectionKey) throws IOException {
    int read = socketChannel.read(readBuffer);
    if (read == -1) {
      cleanupKeyAndTransport(selectionKey);
      return;
    }
    if (read == 0) {
      return;
    }

    logger.trace("Read {} bytes. Buffer capacity: {}", read, readBuffer.capacity());

    readBuffer.flip();
    try {
      if (onDataAvailable != null) {
        // The downstream consumer (TdsPacketFramer) will advance the buffer's position
        // exactly by the number of bytes it consumes.
        onDataAvailable.accept(readBuffer);
      }
    } finally {
      // Any unconsumed bytes (e.g., partial headers or suspended streams) are preserved
      readBuffer.compact();
      logger.trace("Buffer compacted. Position: {}", readBuffer.position());
    }
  }

  private void cleanupKeyAndTransport(SelectionKey key) {
    try {
      key.cancel();
      close();
    } catch (Exception e) {
      logger.warn("Failed to clean up", e);
    }
  }

  @Override
  public void suspendRead() {
    if (selector == null) {
      return;
    }
    SelectionKey key = socketChannel.keyFor(selector);
    if (key != null && key.isValid()) {
      key.interestOpsAnd(~SelectionKey.OP_READ);
      selector.wakeup(); // Force selector to recognize the change immediately
    }
  }

  @Override
  public void resumeRead() {
    if (selector == null) {
      return;
    }
    SelectionKey key = socketChannel.keyFor(selector);
    if (key != null && key.isValid()) {
      key.interestOpsOr(SelectionKey.OP_READ);
      selector.wakeup();
    }
  }

  @Override
  public void close() throws IOException {
    if (eventLoopExecutor != null) {
      eventLoopExecutor.shutdownNow(); // Cleanly shut down the background thread
    }
    if (socketChannel != null && socketChannel.isOpen()) {
      socketChannel.close();
    }
    if (selector != null && selector.isOpen()) {
      selector.close();
    }
  }
}
