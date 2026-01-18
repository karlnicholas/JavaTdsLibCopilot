package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.stream.IIOByteBuffer;
import javax.net.ssl.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;
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
  private ByteBuffer readBuffer = ByteBuffer.allocate(packetSize);

//  private volatile boolean running = true;
  private final Consumer<Message> messageHandler;   // callback from TdsClient
  private final Consumer<Throwable> errorHandler;  // passed by library user

  // Inside TcpTransport class
  private final Queue<ByteBuffer> writeQueue = new ArrayDeque<>();
  private final Object writeLock = new Object();
  private volatile boolean writing = false; // to avoid redundant OP_WRITE

  /**
   * Opens a new TCP connection to the given host and port.
   *
   * @param host remote hostname
   * @param port remote port
   * @throws IOException on I/O error while opening the socket
   */
  public TcpTransport(final String host, final int port, Consumer<Message> messageHandler, Consumer<Throwable> errorHandler) throws IOException {
    this.host = host;
    this.port = port;
    this.messageHandler = messageHandler;
    this.errorHandler = errorHandler;
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
  public void enterAsyncMode() throws IOException {

    this.selector = Selector.open();

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
  private void startEventLoop() throws IOException {
    Thread eventLoopThread = new Thread(() -> {
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

            TcpTransport transport = (TcpTransport) key.attachment();
            if (transport == null) {
              key.cancel();
              continue;
            }

            try {
              if (key.isReadable()) {
                transport.onReadable(key);
              }
              if (key.isWritable()) {
                transport.onWritable();
              }
            } catch (Throwable t) {  // Throwable — catches Error too
              // Per-transport error — usually close this connection
              logger.error("Error processing transport {}", transport, t);
              cleanupKeyAndTransport(key);

              // Optional: notify owner of this specific transport
              errorHandler.accept(t);
            }
          }
        } catch (Throwable fatal) {  // Catch around the whole select loop
          logger.error("Fatal error in TDS event loop", fatal);

          // Critical: notify library user
          errorHandler.accept(fatal);

          // OR: continue; + cleanup all keys  // more resilient but complex
          break;
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
   */
  private void cleanupKeyAndTransport(SelectionKey key) {
    try {
      key.cancel();
      close();
    } catch (Exception e) {
      logger.warn("Failed to clean up key/transport", e);
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
  public void onReadable(SelectionKey selectionKey) throws IOException {
    while (true) {
      // 1. Read as much as possible in one go
      int read = socketChannel.read(readBuffer);

      if (read == -1) {
        logger.info("Connection closed by server (EOF)");

        cleanupKeyAndTransport(selectionKey);
        return;  // Exit immediately
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
          return;
        }

        readBuffer.position(start + length);
      }

      readBuffer.compact();  // shift remaining bytes to front
    }
  }

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
   * Update read timeout for the underlying socket.
   *
   * @param ms timeout in milliseconds
   */
  public void setReadTimeout(final int ms) throws SocketException {
    this.readTimeoutMs = ms;
    socketChannel.socket().setSoTimeout(ms);
  }

  /**
   * Sets the expected packet size. Does not resize buffers currently.
   *
   * @param newSize new packet size in bytes
   */
  public void setPacketSize(final int newSize) {
    this.packetSize = newSize;
    // You may want to resize buffers here in the future
    readBuffer = ByteBuffer.allocate(newSize);
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

  /**
   * Receives a **complete logical response** by reading packets until the last one (EOM).
   *
   * <p>Useful for simple request-response patterns.
   *
   * @return list of all packets that form the logical response
   * @throws IOException if any read fails
   */
  public List<Message> receiveFullResponse() throws IOException {
    List<Message> messages = new ArrayList<>();

    Message packet;
    do {
      packet = receiveSinglePacket();
      messages.add(packet);

      // Optional: handle reset connection flag as soon as we see it
      if (packet.isResetConnection()) {
        // Can notify upper layers immediately if needed
      }
    } while (!packet.isLastPacket());

    return messages;
  }

  /**
   * Receives **one single TDS packet** and wraps it as a Message.
   *
   * <p>This is the most basic receive operation.
   * For full logical responses, the caller should loop until isLastPacket().
   *
   * @return one complete TDS packet as Message
   * @throws IOException if reading fails
   */
  public Message receiveSinglePacket() throws IOException {
    // The packet reader handles header + payload reading
    ByteBuffer rawPacket = readRawPacket();

    // Parse header (first 8 bytes)
    rawPacket.mark();
    final byte type = rawPacket.get();
    final byte status = rawPacket.get();
    final int length = Short.toUnsignedInt(rawPacket.getShort());
    final short spid = rawPacket.getShort();
    final short packetId = rawPacket.getShort();
    rawPacket.get(); // window (usually 0)

    // Reset and slice payload
    rawPacket.reset();
    rawPacket.position(8);
    ByteBuffer payload = rawPacket.slice().limit(length - 8);

    return new Message(
        type,
        status,
        length,
        spid,
        packetId,
        payload,
        System.nanoTime(),
        null  // trace context - can be injected later
    );
  }

  /**
   * Reads exactly one complete TDS packet.
   * Returns the full packet as a ByteBuffer (header + payload).
   */
  public ByteBuffer readRawPacket() throws IOException {
    // First, read the fixed 8-byte header
    ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    readFully(header);
    header.flip();

    byte packetType = header.get();
    byte status = header.get();
    int length = header.getShort() & 0xFFFF;  // unsigned short

    if (length < 8 || length > 32767) {
      throw new IOException("Invalid TDS packet length: " + length);
    }

    // Read the remaining payload
    ByteBuffer payloadBuffer = ByteBuffer.allocate(length - 8).order(ByteOrder.LITTLE_ENDIAN);
    readFully(payloadBuffer);
    payloadBuffer.flip();

    // Combine header + payload into full packet
    ByteBuffer fullPacket = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
    fullPacket.put(header.array());
    fullPacket.put(payloadBuffer.array());
    fullPacket.flip();

    return fullPacket;
  }

  /**
   * Sends a complete logical message (may be split into multiple packets).
   *
   * @param message the message to send (usually built by the client layer)
   * @throws IOException if sending fails
   */
  public void sendMessage(Message message) throws IOException {
    // If the message payload is small, send as single packet
    logger.trace("Sending message {}", logHex(message.getPayload()));
    // If large, split into multiple packets (max ~4096 bytes each)
    List<ByteBuffer> packetBuffers = buildPackets(
        message.getPacketType(),
        message.getStatusFlags(),
        message.getSpid(),
        message.getPayload(),
        (short) 1,
        getCurrentPacketSize()
    );

    for (ByteBuffer buf : packetBuffers) {
      write(buf);
      //  currentPacketNumber++;
    }

  }

  /**
   * Writes the provided buffer to the transport. The buffer's position will be advanced.
   * In blocking mode: direct write.
   * In async mode: queue it and enable OP_WRITE if needed.
   *
   * @param buffer data to write
   * @throws IOException on I/O error
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
    synchronized (writeLock) {
      writeQueue.add(buffer);
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

  /**
   * Builds one or more TDS packets from a payload.
   *
   * @param packetType       TDS message type (e.g. 0x01 for SQL Batch, 0x10 for Login7)
   * @param statusFlags      status flags (usually 0x01 for last packet/EOM)
   * @param payload          the logical message payload (positioned at 0)
   * @param startingPacketId starting packet number (usually 1 for client requests)
   * @param maxPacketSize    maximum allowed packet size (default 4096)
   * @return list of ready-to-send ByteBuffers (each is a full 8-byte header + payload chunk)
   */
  public List<ByteBuffer> buildPackets(
      byte packetType,
      byte statusFlags,
      int spid,
      ByteBuffer payload,
      short startingPacketId,
      int maxPacketSize) {

    List<ByteBuffer> packets = new ArrayList<>();
    short packetId = startingPacketId;

    int maxPayloadPerPacket = maxPacketSize - 8;

    payload = payload.asReadOnlyBuffer();
    payload.rewind();

    boolean isFirst = true;

    while (payload.hasRemaining() || isFirst) {
      isFirst = false;

      int thisPayloadSize = Math.min(maxPayloadPerPacket, payload.remaining());

      // For multi-packet: only last packet has EOM (0x01)
      // For single-packet: always set EOM
      boolean isLast = !payload.hasRemaining() || thisPayloadSize == payload.remaining();
      byte thisStatus = (byte) (isLast ? (statusFlags | 0x01) : (statusFlags & ~0x01));

      ByteBuffer packet = ByteBuffer.allocate(8 + thisPayloadSize)
          .order(ByteOrder.BIG_ENDIAN);

      packet.put(packetType);                    // Byte 0: Type
      packet.put(thisStatus);                    // Byte 1: Status (EOM on last)
      packet.putShort((short) (8 + thisPayloadSize)); // Bytes 2-3: Length (BE)
      packet.putShort((short) spid);                // Bytes 4-5: SPID (0 for client)
      packet.put((byte) (packetId & 0xFF));      // Byte 6: Packet Number (1 byte)
      packet.put((byte) 0);                      // Byte 7: Window (always 0)

      if (thisPayloadSize > 0) {
        ByteBuffer chunk = payload.slice().limit(thisPayloadSize);
        packet.put(chunk);
        payload.position(payload.position() + thisPayloadSize);
      }

      packet.flip();
      packets.add(packet);

      packetId++;
    }

    return packets;
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

  // Helper for hex dumping
  private String  logHex(ByteBuffer buffer) {

    StringBuilder sb = new StringBuilder();
    sb.append(" (Length: ").append(buffer.remaining()).append(")\n");

    int pos = buffer.position();
    int i = 0;
    while (buffer.hasRemaining()) {
      byte b = buffer.get();
      sb.append(String.format("%02X ", b));
      if (++i % 16 == 0) {
        sb.append("\n");
      }
    }
    sb.append("\n");

    // Rewind so the actual write operation can read it again
    buffer.position(pos);
    return sb.toString();
  }

}
