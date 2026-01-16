package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.messages.Message;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * Low-level TCP transport for TDS communication.
 * Supports both plain TCP and TLS (SQL Server encrypted connection).
 */
public class TcpTransport implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TcpTransport.class);

  private final SocketChannel socketChannel;
  private final String host;
  private final int port;

  // TLS fields (null if not using TLS)
  private SSLEngine sslEngine;
  private ByteBuffer myNetData;     // Outgoing encrypted data
  private ByteBuffer peerNetData;   // Incoming encrypted data
  private ByteBuffer peerAppData;   // Decrypted application data

  private static final int TDS_HEADER_LENGTH = 8;
  private static final int PRELOGIN_PACKET_TYPE = 0x12;

  private int readTimeoutMs = 60_000;
  private int packetSize = 4096;  // Default TDS packet size, updated via ENVCHANGE

  private volatile boolean asyncMode = false;
  private Selector selector;           // will be set when entering async mode
  private SelectionKey selectionKey;

  // ── Read state (packet framing) ───────────────────────
  private ByteBuffer readBuffer = ByteBuffer.allocate(32768);

  private volatile boolean running = true;
  private volatile Consumer<Message> messageHandler;   // callback from TdsClient

  /**
   * Opens a new TCP connection to the given host and port.
   *
   * @param host remote hostname
   * @param port remote port
   * @throws IOException on I/O error while opening the socket
   */
  public TcpTransport(final String host, final int port) throws IOException {
    this.host = host;
    this.port = port;
    this.socketChannel = SocketChannel.open();
    this.socketChannel.configureBlocking(true);
    this.socketChannel.socket().setSoTimeout(readTimeoutMs);

    InetSocketAddress address = new InetSocketAddress(host, port);
    if (!socketChannel.connect(address)) {
      socketChannel.finishConnect(); // in case it was non-blocking (rare here)
    }
  }

  /**
   * Switch to non-blocking mode and start delivering complete TDS Messages.
   */
  public void enterAsyncMode(Consumer<Message> handler) throws IOException {
    if (this.messageHandler != null) {
      throw new IllegalStateException("Already in async mode");
    }

    this.selector = Selector.open();  ;
    this.messageHandler = handler;

    socketChannel.configureBlocking(false);
    this.selectionKey = socketChannel.register(selector, SelectionKey.OP_READ, this);

    // Optional: initial interest in reading
    selectionKey.interestOps(SelectionKey.OP_READ);

    asyncMode = true;

    // Optional: clear any leftover TLS buffers to help GC
    myNetData = null;
    peerNetData = null;
    peerAppData = null;
    sslEngine = null;

    startEventLoop();
  }

  /**
   * Start the selector event loop on a background daemon thread.
   * The loop waits for selector events and dispatches readable/writable
   * notifications to the attached transport instance.
   */
  private void startEventLoop() {
    Thread eventLoopThread = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          // Use a reasonable timeout (1s is fine for most cases)
          if (selector.select(1000) == 0) {
            // Idle timeout - can add health checks here if needed
            continue;
          }

          Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
          while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove(); // MUST be called

            if (!key.isValid()) {
              continue;
            }

            TcpTransport transport = (TcpTransport) key.attachment();
            if (transport == null) {
              key.cancel(); // orphan key - clean up
              continue;
            }

            try {
              if (key.isReadable()) {
                transport.onReadable();
              }

              // Writable is rare in TDS client (mostly read-heavy)
              // but good to handle for large sends
              if (key.isWritable()) {
                transport.onWritable();
              }

              // Optional: re-evaluate interest ops after processing
              // (some drivers dynamically adjust OP_READ/OP_WRITE)
              // key.interestOps(SelectionKey.OP_READ);

            } catch (IOException e) {
              // Most common error: connection reset / broken pipe
              logger.warn("I/O error on transport {}: {}", transport, e.getMessage());
              cleanupKeyAndTransport(key, transport);
            } catch (Exception e) {
              // Unexpected protocol / parsing error
              logger.error("Unexpected error processing key for {}", transport, e);
              cleanupKeyAndTransport(key, transport);
            }
          }
        }
      } catch (ClosedSelectorException e) {
        logger.debug("Selector closed normally during shutdown");
      } catch (Exception e) {
        logger.error("Catastrophic failure in TDS event loop", e);
        // Optional: notify all transports of fatal error
      } finally {
        // Final cleanup (good practice)
        try {
          selector.close();
        } catch (Exception ignored) {
          logger.warn("Failed to close selector", ignored);
        }
      }
    }, "TDS-EventLoop-" + host + ":" + port);

    eventLoopThread.setDaemon(true);
    eventLoopThread.start();
  }

  /**
   * Cancel the provided selection key and close the associated transport.
   * Exceptions during cleanup are logged and suppressed to avoid crashing the event loop.
   *
   * @param key       the selection key to cancel
   * @param transport the transport to close
   */
  private void cleanupKeyAndTransport(SelectionKey key, TcpTransport transport) {
    try {
      key.cancel();
      transport.close();
    } catch (Exception e) {
      logger.warn("Failed to clean up key/transport", e);
    }
  }

  /**
   * Writes the provided buffer to the transport. The buffer's position will be advanced.
   *
   * @param buffer data to write
   * @throws IOException on I/O error
   */

  // Inside TcpTransport class
  private final Queue<ByteBuffer> writeQueue = new ArrayDeque<>();
  private final Object writeLock = new Object();
  private volatile boolean writing = false; // to avoid redundant OP_WRITE

  /**
   * Writes the buffer. In blocking mode: direct write.
   * In async mode: queue it and enable OP_WRITE if needed.
   */
  public void write(ByteBuffer buffer) throws IOException {
    if (!asyncMode) {
      // Blocking path (connect/login phase)
      if (sslEngine != null) {
        writeEncrypted(buffer);
      } else {
        while (buffer.hasRemaining()) {
          socketChannel.write(buffer);
        }
      }
      return;
    }

    // Async mode: queue the buffer (copy to avoid mutation)
    ByteBuffer copy = buffer.duplicate(); // safe copy
    synchronized (writeLock) {
      writeQueue.add(copy);
    }

    // Tell event loop we have data to write
    SelectionKey key = socketChannel.keyFor(selector);
    if (key != null && key.isValid()) {
      synchronized (writeLock) {
        if (!writing) {
          writing = true;
          key.interestOpsOr(SelectionKey.OP_WRITE);
          selector.wakeup(); // wake selector if sleeping
        }
      }
    }
  }

  /**
   * Called from event loop when key.isWritable().
   */
  public void onWritable() throws IOException {
    synchronized (writeLock) {
      while (!writeQueue.isEmpty()) {
        ByteBuffer buf = writeQueue.peek();
        int written = socketChannel.write(buf);

        if (written == 0) {
          // Can't write more now — keep OP_WRITE
          break;
        }

        if (!buf.hasRemaining()) {
          writeQueue.poll(); // fully sent
        }
      }

      // If queue empty → drop OP_WRITE
      if (writeQueue.isEmpty()) {
        writing = false;
        SelectionKey key = socketChannel.keyFor(selector);
        if (key != null && key.isValid()) {
          key.interestOpsAnd(~SelectionKey.OP_WRITE);
        }
      }
    }
  }

  /**
   * Handle readable selector events: read available bytes, assemble full TDS packets,
   * convert them into Message objects and deliver to the registered message handler.
   * This method will emit an EOF message and stop the transport if the remote closes
   * the connection, and will stop delivering messages on unrecoverable parse errors.
   *
   * @throws IOException on I/O error while reading from the socket
   */
  public void onReadable() throws IOException {
    if (!running || messageHandler == null) {
      logger.debug("Skipping read - transport not running or no handler");
      return;
    }

    while (true) {
      // 1. Read as much as possible in one go
      int read = socketChannel.read(readBuffer);

      if (read == -1) {
        logger.info("Connection closed by server (EOF)");
        running = false;
        if (messageHandler != null) {
          messageHandler.accept(createEofMessage());
        }
        return;
      }

      if (read == 0) {
        break;  // nothing available right now
      }

      logger.trace("Read {} bytes (total: {})", read, readBuffer.position());

      readBuffer.flip();

      // 2. Process all complete packets in this read
      while (readBuffer.remaining() >= TDS_HEADER_LENGTH) {  // use constant
        int start = readBuffer.position();

        // Quick peek at length without advancing position
        int length = Short.toUnsignedInt(readBuffer.getShort(start + 2));

        if (readBuffer.remaining() < length) {
          // Not enough for full packet → wait for next read
          break;
        }

        // 3. Extract complete packet
        ByteBuffer packet = readBuffer.slice(start, length);
        packet.position(0);

        try {
          Message message = buildMessageFromPacket(packet);
          if (messageHandler != null) {
            messageHandler.accept(message);
          }
        } catch (Exception e) {
          logger.error("Failed to build message from packet", e);
          // Optional: signal error upstream or close connection
          // subscriber.onError(e);  // if using reactive
          running = false;
          return;
        }

        readBuffer.position(start + length);
      }

      readBuffer.compact();  // shift remaining bytes to front
    }
  }

  private Message buildMessageFromPacket(ByteBuffer packet) {
    packet.mark();

    final byte type   = packet.get();
    final byte status = packet.get();
    final int length  = Short.toUnsignedInt(packet.getShort());
    final short spid  = packet.getShort();
    final byte packetId = packet.get(); // actually a byte, not short
    final byte window = packet.get();   // usually 0

    packet.reset();
    packet.position(8);

    ByteBuffer payload = packet.slice();
    payload.limit(length - 8);

    return new Message(
            type,
            status,
            length,
            spid,
            packetId,
            payload,
            System.nanoTime(),
            null   // trace context
    );
  }

  // Optional: special message for EOF / connection close
  private Message createEofMessage() {
    return new Message(
            (byte) 0xFF, // special marker
            (byte) 0x00,
            0,
            (short) 0,
            (byte) 0,
            ByteBuffer.allocate(0),
            System.nanoTime(),
            null
    );
  }

  // Optional helpers
  public boolean isConnected() {
    return socketChannel.isConnected();
  }

  public SocketChannel getChannel() {
    return socketChannel;
  }

  public boolean isAsyncMode() {
    return asyncMode;
  }

  // Very important cleanup
  @Override
  public void close() throws IOException {
    asyncMode = false;
    if (selectionKey != null) {
      selectionKey.cancel();
    }
    if (selector != null) {
      selector.close();
    }
    if (sslEngine != null) {
      try {
        sslEngine.closeOutbound();
      } catch (Exception ignored) {
        logger.warn("Error during SSL engine close", ignored);
      }
    }
    socketChannel.close();
  }

  // ... rest of your existing methods ...
  /**
   * Enable TLS on the existing connection and perform the TLS handshake.
   *
   * @throws IOException on TLS initialization or handshake failures
   */
  public void enableTls() throws IOException {
    try {
      // 1. Trust All Certs (Explicitly requested)
      final TrustManager[] trustAllCerts = new TrustManager[]{
          new X509TrustManager() {
            @Override
            public X509Certificate[] getAcceptedIssuers() {
              return new X509Certificate[0];
            }

            @Override
            public void checkClientTrusted(final X509Certificate[] certs,
                                           final String authType) {
              // Trust all
            }

            @Override
            public void checkServerTrusted(final X509Certificate[] certs,
                                           final String authType) {
              // Trust all
            }
          }
      };

      // 2. Init SSLContext (TLSv1.2)
      final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
      sslContext.init(null, trustAllCerts, new SecureRandom());

      this.sslEngine = sslContext.createSSLEngine(host, port);
      this.sslEngine.setUseClientMode(true);

      final SSLSession session = sslEngine.getSession();
      // Allocate buffers. peerNetData must hold TDS packets + TLS records.
      final int bufferSize = Math.max(session.getPacketBufferSize(), 32768);

      myNetData = ByteBuffer.allocate(bufferSize);
      peerNetData = ByteBuffer.allocate(bufferSize);
      peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize());

      // Prepare for reading
      peerNetData.flip();

      sslEngine.beginHandshake();
      doHandshake();

    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new IOException("TLS initialization failed", e);
    }
  }

  private void doHandshake() throws IOException {
    SSLEngineResult result;
    SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
    final ByteBuffer dummy = ByteBuffer.allocate(0);

    // Helper buffer for reading the TDS header during handshake
    final ByteBuffer headerBuf = ByteBuffer.allocate(TDS_HEADER_LENGTH);

    while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
        && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {

      switch (handshakeStatus) {
        case NEED_UNWRAP:
          // TLS records are wrapped inside TDS 0x12 packets.

          // Only read from network if buffer has no remaining data.
          if (!peerNetData.hasRemaining()) {
            peerNetData.clear();
            headerBuf.clear();

            // 1. Read TDS header
            readFully(headerBuf);
            headerBuf.flip();

            // 2. Parse length (bytes 2-3 big-endian)
            final int packetLength = Short.toUnsignedInt(headerBuf.getShort(2));
            final int tlsDataLength = packetLength - TDS_HEADER_LENGTH;

            // 3. Read the TLS payload inside the TDS packet
            peerNetData.limit(tlsDataLength);
            readFully(peerNetData);
            peerNetData.flip();
          }

          try {
            result = sslEngine.unwrap(peerNetData, peerAppData);

            // Handle BUFFER_UNDERFLOW (TLS record split across TDS packets)
            if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
              peerNetData.compact();

              // Read next TDS header
              headerBuf.clear();
              readFully(headerBuf);
              headerBuf.flip();

              final int packetLength = Short.toUnsignedInt(headerBuf.getShort(2));
              final int tlsDataLength = packetLength - TDS_HEADER_LENGTH;

              final int limit = peerNetData.position() + tlsDataLength;
              if (limit > peerNetData.capacity()) {
                throw new IOException("Buffer overflow while reading TLS payload");
              }
              peerNetData.limit(limit);
              readFully(peerNetData);

              peerNetData.flip(); // Retry unwrap
            }
          } catch (final SSLException e) {
            throw new IOException("TLS Handshake unwrap failed", e);
          }
          handshakeStatus = result.getHandshakeStatus();
          break;

        case NEED_WRAP:
          myNetData.clear();
          // Reserve 8 bytes for TDS header
          myNetData.position(TDS_HEADER_LENGTH);

          try {
            result = sslEngine.wrap(dummy, myNetData);
            handshakeStatus = result.getHandshakeStatus();
          } catch (final SSLException e) {
            throw new IOException("TLS Handshake wrap failed", e);
          }

          myNetData.flip();
          final int totalLength = myNetData.limit();

          // Add TDS header (0x12 Pre-Login)
          myNetData.put(0, (byte) PRELOGIN_PACKET_TYPE);
          myNetData.put(1, (byte) 0x01);
          myNetData.putShort(2, (short) totalLength);
          myNetData.putShort(4, (short) 0x0000);
          myNetData.put(6, (byte) 0x01);
          myNetData.put(7, (byte) 0x00);

          while (myNetData.hasRemaining()) {
            socketChannel.write(myNetData);
          }
          break;

        case NEED_TASK:
          Runnable task;
          while ((task = sslEngine.getDelegatedTask()) != null) {
            task.run();
          }
          handshakeStatus = sslEngine.getHandshakeStatus();
          break;

        default:
          throw new IllegalStateException(
              "Invalid TLS Handshake status: " + handshakeStatus);
      }
    }
  }

  /**
   * Returns true if TLS is enabled on this transport.
   *
   * @return {@code true} when TLS is enabled
   */
  public boolean isSecure() {
    return sslEngine != null;
  }

  private void writeEncrypted(final ByteBuffer appData) throws IOException {
    while (appData.hasRemaining()) {
      myNetData.clear();

      final SSLEngineResult result = sslEngine.wrap(appData, myNetData);

      if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
        // Double buffer size and retry
        myNetData = ByteBuffer.allocate(myNetData.capacity() * 2);
        continue;
      }

      myNetData.flip();
      while (myNetData.hasRemaining()) {
        socketChannel.write(myNetData);
      }
    }
  }

  /**
   * Reads a full TDS packet (including header) into the provided buffer.
   * For TLS, this returns the decrypted application data.
   *
   * @param buffer destination buffer
   * @throws IOException on I/O error or end-of-stream
   */
  public void readFully(final ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      final int read = socketChannel.read(buffer);
      if (read == -1) {
        throw new IOException("Unexpected end of stream");
      }
    }
  }

  private void readDecryptedFully(final ByteBuffer target) throws IOException {
    while (target.hasRemaining()) {
      if (peerAppData.remaining() == 0) {
        peerAppData.clear();

        // Read more encrypted data if needed
        if (!peerNetData.hasRemaining()) {
          peerNetData.clear();
          final int count = socketChannel.read(peerNetData);
          if (count == -1) {
            throw new IOException("Connection closed");
          }
          peerNetData.flip();
        }

        final SSLEngineResult result = sslEngine.unwrap(peerNetData, peerAppData);

        if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
          peerNetData.compact();
          final int count = socketChannel.read(peerNetData);
          if (count == -1) {
            throw new IOException("Connection closed");
          }
          peerNetData.flip();
        }
      }

      // Copy available decrypted data to target
      final int toCopy = Math.min(target.remaining(), peerAppData.remaining());
      final byte[] tmp = new byte[toCopy];
      peerAppData.get(tmp);
      target.put(tmp);
    }
  }

  /**
   * No-op flush for this transport.
   */
  public void flush() {
    // No-op for plain TCP; TLS doesn't need explicit flush in this design
  }

  /**
   * Update read timeout for the underlying socket.
   *
   * @param ms timeout in milliseconds
   */
  public void setReadTimeout(final int ms) {
    this.readTimeoutMs = ms;
    try {
      socketChannel.socket().setSoTimeout(ms);
    } catch (final IOException e) {
      logger.warn("Failed to set socket timeout", e);
    }
  }

  /**
   * Sets the expected packet size. Does not resize buffers currently.
   *
   * @param newSize new packet size in bytes
   */
  public void setPacketSize(final int newSize) {
    this.packetSize = newSize;
    // You may want to resize buffers here in the future
  }

  /**
   * Returns the current packet size.
   *
   * @return packet size in bytes
   */
  public int getCurrentPacketSize() {
    return packetSize;
  }

  /**
   * Disable TLS mode (does not close connection).
   */
  public void disableTls() {
    sslEngine = null;
  }

  public String getHost() {
    return host;
  }

}
