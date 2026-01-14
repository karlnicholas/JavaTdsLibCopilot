package org.tdslib.javatdslib.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.messages.Message;

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
   * Switch to non-blocking mode and start delivering complete TDS Messages
   */
  public void enterAsyncMode(Selector selector, Consumer<Message> handler) throws IOException {
    if (this.messageHandler != null) {
      throw new IllegalStateException("Already in async mode");
    }

    this.selector = selector;
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
  }


  /**
   * Writes the provided buffer to the transport. The buffer's position will be advanced.
   *
   * @param buffer data to write
   * @throws IOException on I/O error
   */
  public void write(ByteBuffer buffer) throws IOException {
    if (!asyncMode) {
      // Old blocking path (used during connect/login)
      if (sslEngine != null) {
        writeEncrypted(buffer);
      } else {
        while (buffer.hasRemaining()) {
          socketChannel.write(buffer);
        }
      }
    } else {
      // In async mode we DON'T write directly from this method!
      // → The caller (usually event loop / selector thread) should do it
      throw new IllegalStateException(
              "Direct write not allowed in async mode - use queue + selector");
    }
  }
//  public void write(ByteBuffer buffer) throws IOException {
//    while (buffer.hasRemaining()) {
//      socketChannel.write(buffer);
//    }
//  }
//
  public void onReadable() throws IOException {
    if (!running || messageHandler == null) return;

    while (true) {
      int read = socketChannel.read(readBuffer);
      if (read == -1) {
        // Connection closed by peer
        running = false;
        messageHandler.accept(createEofMessage());
        return;
      }
      if (read == 0) {
        break; // nothing more to read right now
      }

      readBuffer.flip();

      while (readBuffer.remaining() >= 8) { // at least header size
        int start = readBuffer.position();

        // Parse minimal header to get length
        byte type = readBuffer.get(start);
        byte status = readBuffer.get(start + 1);
        int length = Short.toUnsignedInt(readBuffer.getShort(start + 2));

        if (readBuffer.remaining() < length) {
          // Incomplete packet - wait for more data
          break;
        }

        // We have a complete packet → slice it
        ByteBuffer packet = readBuffer.slice(start, length);
        packet.position(0); // ready to read from beginning

        Message message = buildMessageFromPacket(packet);
        messageHandler.accept(message);

        // Advance buffer past this packet
        readBuffer.position(start + length);
      }

      // Prepare buffer for next read
      readBuffer.compact();
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

//  @Override
//  public void close() throws IOException {
//    running = false;
//    if (selectionKey != null) selectionKey.cancel();
//    if (socketChannel != null) socketChannel.close();
//    if (selector != null) selector.close();
//  }

  // Optional helpers
  public boolean isConnected() {
    return socketChannel.isConnected();
  }

  public SocketChannel getChannel() {
    return socketChannel;
  }

  public void onWritable() throws IOException {
    // Handle pending outgoing data if you have a write queue
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
      } catch (Exception ignored) {}
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
