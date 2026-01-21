package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.TdsVersion;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Low-level TCP transport for TDS communication.
 * Supports both plain TCP and TLS (SQL Server encrypted connection).
 */
public class TdsTransport implements ConnectionContext, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TdsTransport.class);
  private static final String SENDING_MESSAGE = "Sending TDS message {}";

  private final SocketChannel socketChannel;
  private final String host;
  private final int port;
  private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean pendingWrite = new AtomicBoolean(false);

  private static final int TDS_HEADER_LENGTH = 8;

  private int readTimeoutMs = 60_000;

  private Selector selector;           // will be set when entering async mode

  // ── Read state (packet framing) ───────────────────────
  private ByteBuffer readBuffer;

  private Consumer<TdsMessage> currentMessageHandler;   // callback from TdsClient
  private Consumer<Throwable> currentErrorHandler;  // passed by library user
  private final TlsHandshake tlsHandshake;

  private TdsVersion tdsVersion = TdsVersion.V7_4;
  private String currentDatabase;
  private String currentLanguage;
  private String currentCharset;
  private int packetSize = 4096;
  private byte[] currentCollationBytes;
  private boolean inTransaction;
  private String serverName;
  private String serverVersionString;
  private int spid;

  private static final Map<Integer, String> COMMON_SORTID_NAMES = new HashMap<>();

  static {
    COMMON_SORTID_NAMES.put(52, "SQL_Latin1_General_CP1_CI_AS (Sort Order 52)");
    COMMON_SORTID_NAMES.put(51, "SQL_Latin1_General_CP1_CS_AS");
    COMMON_SORTID_NAMES.put(54, "SQL_Latin1_General_CP850_CI_AS");
    COMMON_SORTID_NAMES.put(53, "SQL_Latin1_General_CP1_CI_AI");
    COMMON_SORTID_NAMES.put(55, "SQL_Latin1_General_CP850_CS_AS");
    // You can add more legacy SQL collations from sys.collations where name LIKE 'SQL_%'
    // Full list: https://learn.microsoft.com/en-us/sql/relational-databases/collations/sql-server-collations
  }

  /**
   * Opens a new TCP connection to the given host and port.
   *
   * @param host remote hostname
   * @param port remote port
   * @throws IOException on I/O error while opening the socket
   */
  public TdsTransport(
      final String host,
      final int port
  ) throws IOException {
    this.host = host;
    this.port = port;
    this.tlsHandshake = new TlsHandshake();
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
    readBuffer = ByteBuffer.allocate(packetSize);
    this.selector = Selector.open();

    socketChannel.configureBlocking(false);
    socketChannel.register(selector, SelectionKey.OP_READ, this);

    startEventLoop();
  }

  /**
   * Start the selector event loop on a background daemon thread.
   * The loop waits for selector events and dispatches readable/writable
   * notifications to the attached transport instance.
   */
  private void startEventLoop() {
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

            TdsTransport transport = (TdsTransport) key.attachment();
            if (transport == null) {
              key.cancel();
              continue;
            }

            try {
              if (key.isReadable()) {
                transport.onReadable(key);
              }
              if (key.isWritable()) {
                transport.onWritable(key);
              }
            } catch (Throwable t) {  // Throwable — catches Error too
              // Per-transport error — usually close this connection
              logger.error("Error processing transport {}", transport, t);
              cleanupKeyAndTransport(key);

              // Optional: notify owner of this specific transport
              currentErrorHandler.accept(t);
            }
          }
        } catch (Throwable fatal) {  // Catch around the whole select loop
          logger.error("Fatal error in TDS event loop", fatal);

          // Critical: notify library user
          currentErrorHandler.accept(fatal);

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
  private void onWritable(SelectionKey key) throws IOException {
    SocketChannel ch = (SocketChannel) key.channel();

    while (true) {
      ByteBuffer buf = writeQueue.peek();
      if (buf == null) {
        // Queue is empty → we can safely turn off OP_WRITE
        pendingWrite.set(false);
        key.interestOpsAnd(~SelectionKey.OP_WRITE);
        return;
      }

      int written = ch.write(buf);

      if (written == 0) {
        // Can't write more right now
        return;
      }

      if (buf.hasRemaining()) {
        // Still data left in current buffer → stay interested
        return;
      }

      // Buffer fully written → remove it
      writeQueue.poll();
    }
  }

  /**
   * Handle readable selector events: read available bytes, assemble full TDS packets,
   * convert them into TdsMessage objects and deliver to the registered message handler.
   * This method will emit an EOF message and stop the transport if the remote closes
   * the connection, and will stop delivering messages on unrecoverable parse errors.
   *
   * @throws IOException on I/O error while reading from the socket
   */
  public void onReadable(SelectionKey selectionKey) throws IOException {
    // 1. Read once (usually enough to fill buffer)
    int read = socketChannel.read(readBuffer);

    if (read == -1) {
      logger.info("Connection closed by server (EOF)");
      cleanupKeyAndTransport(selectionKey);
      return;
    }
    if (read == 0) {
      return;
    }

    readBuffer.flip();

    try {
      while (readBuffer.remaining() >= TDS_HEADER_LENGTH) {
        int length = Short.toUnsignedInt(readBuffer.getShort(readBuffer.position() + 2));

        // Safety Check: Packet larger than buffer?
        if (length > readBuffer.capacity()) {
          throw new IOException("TDS Packet length (" + length + ") exceeds buffer capacity");
        }

        if (readBuffer.remaining() < length) {
          break; // Wait for more data
        }

        // Slice and process
        ByteBuffer packet = readBuffer.slice().limit(length);

        // Advance position immediately so we don't re-read this header
        readBuffer.position(readBuffer.position() + length);

        TdsMessage tdsMessage = buildMessageFromPacket(packet);
        currentMessageHandler.accept(tdsMessage);
      }
    } finally {
      readBuffer.compact();
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
   * Receives a **complete logical response** by reading packets until the last one (EOM).
   *
   * <p>Useful for simple request-response patterns.
   *
   * @return list of all packets that form the logical response
   * @throws IOException if any read fails
   */
  public List<TdsMessage> receiveFullResponse() throws IOException {
    List<TdsMessage> tdsMessages = new ArrayList<>();

    TdsMessage packet;
    do {
      packet = receiveSinglePacket();
      tdsMessages.add(packet);

      // Optional: handle reset connection flag as soon as we see it
      if (packet.isResetConnection()) {
        // Can notify upper layers immediately if needed
      }
    } while (!packet.isLastPacket());

    return tdsMessages;
  }

  public TdsMessage buildMessageFromPacket(ByteBuffer packet) {
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

    return new TdsMessage(
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

  /**
   * Receives **one single TDS packet** and wraps it as a TdsMessage.
   *
   * <p>This is the most basic receive operation.
   * For full logical responses, the caller should loop until isLastPacket().
   *
   * @return one complete TDS packet as TdsMessage
   * @throws IOException if reading fails
   */
  public TdsMessage receiveSinglePacket() throws IOException {
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

    return new TdsMessage(
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

  /**
   * Sends a complete logical tdsMessage (may be split into multiple packets).
   *
   * @param tdsMessage the tdsMessage to send (usually built by the client layer)
   * @throws IOException if sending fails
   */
  public void sendMessageDirect(TdsMessage tdsMessage) throws IOException {
    logger.trace(SENDING_MESSAGE, logHex(tdsMessage.getPayload()));

    QueryPacketBuilder queryPacketBuilder = new QueryPacketBuilder();
    // If large, split into multiple packets (max ~4096 bytes each)
    List<ByteBuffer> packetBuffers = queryPacketBuilder.buildPackets(
        tdsMessage.getPacketType(),
        tdsMessage.getStatusFlags(),
        getSpid(),
        tdsMessage.getPayload(),
        (short) 1,
        getCurrentPacketSize()
    );

    for (ByteBuffer buf : packetBuffers) {
      writeDirect(buf);
      //  currentPacketNumber++;
    }

  }

  /**
   * Sets client handlers for incoming messages and transport errors.
   *
   * <p>The transport will invoke {@code currentMessageHandler.accept(tdsMessage)} for each
   * complete {@link TdsMessage} received, and {@code currentErrorHandler.accept(throwable)}
   * for transport-level errors (selector loop, I/O, parsing, etc.).
   *
   * <p>Handlers may be invoked on the transport's event loop thread; callers should ensure
   * the provided handlers are thread-safe or offload work to another thread as needed.
   *
   * @param currentMessageHandler callback to receive complete {@link TdsMessage}
   *                              instances; must not be null
   * @param currentErrorHandler callback to receive transport errors; must not be null
   */
  public void setClientHandlers(
      Consumer<TdsMessage> currentMessageHandler,
      Consumer<Throwable> currentErrorHandler) {
    this.currentMessageHandler = currentMessageHandler;
    this.currentErrorHandler = currentErrorHandler;
  }

  /**
   * Sends a complete logical tdsMessage (may be split into multiple packets) asynchronously.
   *
   * @param tdsMessage the tdsMessage to send (usually built by the client layer)
   * @throws IOException if sending fails
   */
  public void sendQueryMessageAsync(TdsMessage tdsMessage) throws IOException {
    logger.trace(SENDING_MESSAGE, logHex(tdsMessage.getPayload()));
    QueryPacketBuilder queryPacketBuilder = new QueryPacketBuilder();
    List<ByteBuffer> packetBuffers = queryPacketBuilder.buildPackets(
            tdsMessage.getPacketType(),
            tdsMessage.getStatusFlags(),
            getSpid(),
            tdsMessage.getPayload(),
            (short) 1,
            getCurrentPacketSize()
    );
//    // If large, split into multiple packets (max ~4096 bytes each)
//    List<ByteBuffer> packetBuffers = buildPackets(
//        tdsMessage.getPacketType(),
//        tdsMessage.getStatusFlags(),
//        tdsMessage.getSpid(),
//        tdsMessage.getPayload(),
//        (short) 1,
//        getCurrentPacketSize()
//    );

    for (ByteBuffer buf : packetBuffers) {
      writeAsync(buf);
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
  public void writeDirect(ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      socketChannel.write(buffer);
    }
  }

  /**
   * Writes the provided buffer to the transport. The buffer's position will be advanced.
   * In blocking mode: direct write.
   * In async mode: queue it and enable OP_WRITE if needed.
   *
   * @param src data to write
   */
  public void writeAsync(ByteBuffer src) {
    SelectionKey key;
    // Important: we must copy because caller may reuse the buffer!
    ByteBuffer copy = src.duplicate(); // or src.slice() + rewind if you prefer

    boolean wasEmpty = writeQueue.isEmpty();
    writeQueue.offer(copy);

    // Only one thread needs to turn OP_WRITE on
    if (wasEmpty && pendingWrite.compareAndSet(false, true)) {
      key = socketChannel.keyFor(selector);
      if (key != null && key.isValid()) {
        key.interestOpsOr(SelectionKey.OP_WRITE);
        selector.wakeup();
      }
    }
  }

  // Very important cleanup
  @Override
  public void close() throws IOException {
    if (selector != null) {
      selector.close();
    }
    socketChannel.close();
  }

  // Helper for hex dumping
  private String logHex(ByteBuffer buffer) {

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

  /**
   * Performs a TLS handshake on the underlying socket channel to establish
   * an encrypted session with the remote host.
   *
   * <p>This delegates to the {@link TlsHandshake} helper which performs the
   * protocol negotiation and any necessary I/O. The call may block until the
   * handshake completes or fails.</p>
   *
   * @throws IOException if an I/O error occurs during the handshake
   * @throws NoSuchAlgorithmException if the required SSL/TLS algorithm is not available
   * @throws KeyManagementException if there is an error initializing key management for TLS
   */
  public void tlsHandshake() throws IOException, NoSuchAlgorithmException, KeyManagementException {
    tlsHandshake.tlsHandshake(host, port, socketChannel);
  }

  /**
   * Finalize TLS setup and release handshake resources.
   *
   * <p>Call this after a successful {@link #tlsHandshake()} to indicate that the
   * TLS handshake has completed and handshake helper resources can be closed.
   * After this call, the transport is expected to use the established TLS session
   * for subsequent I/O. Any I/O errors while releasing handshake resources will
   * be reported via the thrown {@link IOException}.</p>
   *
   */
  public void tlsComplete() {
    tlsHandshake.close();
  }

  /**
   * Sends a logical TDS tdsMessage over the established TLS session.
   *
   * <p>The tdsMessage is split into one or more TDS packets via
   * Each resulting packet is then encrypted and written
   * to the underlying {@link java.nio.channels.SocketChannel} via the handshake helper.
   *
   * <p>Preconditions:
   * <ul>
   *   <li>A successful TLS handshake must have been performed so that
   *   {@link #tlsHandshake} can encrypt data.</li>
   *   <li>The {@code tdsMessage} payload buffer should be positioned appropriately;
   *   this method does not rewind the caller's buffer.</li>
   * </ul>
   *
   * @param tdsMessage the logical tdsMessage to send (over TLS)
   * @throws IOException if encryption or socket I/O fails while sending packets
   */
  public void sendMessageEncrypted(TdsMessage tdsMessage) throws IOException {
    logger.trace(SENDING_MESSAGE, logHex(tdsMessage.getPayload()));
    // If large, split into multiple packets (max ~4096 bytes each)
//    List<ByteBuffer> packetBuffers = buildPackets(
//        tdsMessage.getPacketType(),
//        tdsMessage.getStatusFlags(),
//        tdsMessage.getSpid(),
//        tdsMessage.getPayload(),
//        (short) 1,
//        getCurrentPacketSize()
//    );

    QueryPacketBuilder queryPacketBuilder = new QueryPacketBuilder();
    List<ByteBuffer> packetBuffers = queryPacketBuilder.buildPackets(
            tdsMessage.getPacketType(),
            tdsMessage.getStatusFlags(),
            getSpid(),
            tdsMessage.getPayload(),
            (short) 1,
            getCurrentPacketSize()
    );

    for (ByteBuffer buffer : packetBuffers) {
      tlsHandshake.writeEncrypted(buffer, socketChannel);
    }
  }

  /**
   * Cancels the current asynchronous operation.
   */
  public void cancelCurrent() {
  }

  @Override
  public TdsVersion getTdsVersion() {
    return tdsVersion;
  }

  @Override
  public void setTdsVersion(TdsVersion version) {
    this.tdsVersion = version;
  }

  @Override
  public boolean isUnicodeEnabled() {
    return tdsVersion.ordinal() >= TdsVersion.V7_1.ordinal();
  }

  @Override
  public String getCurrentDatabase() {
    return currentDatabase;
  }

  @Override
  public void setDatabase(String database) {
    this.currentDatabase = database;
  }

  @Override
  public String getCurrentLanguage() {
    return currentLanguage;
  }

  @Override
  public void setLanguage(String language) {
    this.currentLanguage = language;
  }

  @Override
  public String getCurrentCharset() {
    return currentCharset;
  }

  @Override
  public void setCharset(String charset) {
    this.currentCharset = charset;
  }

  @Override
  public int getCurrentPacketSize() {
    return packetSize;
  }

  @Override
  public void setPacketSize(int size) {
    this.packetSize = size;
    // Already doing this — keep it consistent
    // You may want to resize readBuffer here in the future
  }

  @Override
  public byte[] getCurrentCollationBytes() {
    return Arrays.copyOf(currentCollationBytes, currentCollationBytes.length);
  }

  @Override
  public void setCollationBytes(byte[] collationBytes) {
    this.currentCollationBytes = collationBytes != null
        ? Arrays.copyOf(collationBytes, collationBytes.length)
        : new byte[0];
  }

  @Override
  public boolean isInTransaction() {
    return inTransaction;
  }

  @Override
  public void setInTransaction(boolean inTransaction) {
    this.inTransaction = inTransaction;
  }

  @Override
  public String getServerName() {
    return serverName;
  }

  @Override
  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  @Override
  public String getServerVersionString() {
    return serverVersionString;
  }

  @Override
  public void setServerVersionString(String versionString) {
    this.serverVersionString = versionString;
  }

  @Override
  public int getSpid() {
    return spid;
  }

  @Override
  public void setSpid(int spid) {
    logger.debug("Setting SPID to {}", spid);
    this.spid = spid;
  }

  @Override
  public void resetToDefaults() {
    currentDatabase = null;
    currentLanguage = "us_english";
    currentCharset = null;
    packetSize = 4096;
    currentCollationBytes = new byte[0];
    inTransaction = false;
    spid = 0;
    // Important: do NOT reset tdsVersion, serverName, serverVersionString
    logger.debug("Session state reset due to resetConnection flag");
  }

  /**
   * Applies an ENVCHANGE token to the connection context.
   * Decodes values correctly based on EnvChangeType (char count vs bytes, UTF-16LE vs ASCII).
   */
  public void applyEnvChange(EnvChangeToken change) {
    EnvChangeType type = change.getChangeType();

    // Determine charset for string-based changes (TDS 7.0+ uses UTF-16LE)
    Charset charset = isUnicodeEnabled()
        ? StandardCharsets.UTF_16LE
        : StandardCharsets.US_ASCII;

    ByteBuffer buf = ByteBuffer.wrap(change.getValueBytes());

    switch (type) {
      case DATABASE:
      case LANGUAGE:
      case CHARSET:
        // New value first (char count + data)
        int dbNewCharLen = buf.get() & 0xFF;
        byte[] dbNewData = new byte[dbNewCharLen * 2];
        buf.get(dbNewData);
        String dbNewValue = new String(dbNewData, charset).trim();

        // Old value second
        int dbOldCharLen = buf.get() & 0xFF;
        byte[] dbOldData = new byte[dbOldCharLen * 2];
        buf.get(dbOldData);
        String dbOldValue = new String(dbOldData, charset).trim();

        if (type == EnvChangeType.DATABASE) {
          setDatabase(dbNewValue);
          logger.info("Database changed from '{}' to '{}'", dbOldValue, dbNewValue);
        } else if (type == EnvChangeType.LANGUAGE) {
          setLanguage(dbNewValue);
          logger.info("Language changed from '{}' to '{}'", dbOldValue, dbNewValue);
        } else if (type == EnvChangeType.CHARSET) {
          setCharset(dbNewValue);
          logger.info("Charset changed from '{}' to '{}'", dbOldValue, dbNewValue);
        }
        break;

      case PACKET_SIZE:
      case PACKET_SIZE_ALT:
        int sizeCharLen = buf.get() & 0xFF;
        byte[] sizeBytes = new byte[sizeCharLen * 2];
        buf.get(sizeBytes);
        String sizeStr = new String(sizeBytes, charset).trim();
        try {
          int newSize = Integer.parseInt(sizeStr);
          if (newSize >= 512 && newSize <= 32767) {
            setPacketSize(newSize);
            logger.info("Packet size changed to {}", newSize);
          } else {
            logger.warn("Invalid packet size: {}", newSize);
          }
        } catch (NumberFormatException e) {
          logger.warn("Failed to parse packet size: '{}'", sizeStr);
        }
        break;

      case SQL_COLLATION:
        // New collation
        int newInfoLen = buf.get() & 0xFF;
        byte[] newCollationData = new byte[newInfoLen];
        buf.get(newCollationData);

        // Old collation
        int oldInfoLen = buf.get() & 0xFF;
        byte[] oldCollationData = new byte[oldInfoLen];
        buf.get(oldCollationData);

        // Store the new collation data raw (most clients do this)
        setCollationBytes(newCollationData);

        // Decode and log meaningful collation info
        if (newInfoLen >= 5) {
          ByteBuffer collationBuf = ByteBuffer.wrap(newCollationData)
              .order(ByteOrder.LITTLE_ENDIAN);

          final int fullLcid = collationBuf.getInt(); // bytes 0-3: LCID + flags
          final byte verSortByte = collationBuf.get(); // byte 4: version+sort nibble

          final int flags = (fullLcid >>> 20) & 0xFF; // sensitivity flags

          final int fullSortOrderId = verSortByte & 0xFF;
          final int versionNibble = (fullSortOrderId >>> 4) & 0x0F;
          final int sortNibble = fullSortOrderId & 0x0F;

          final String friendlyName = COMMON_SORTID_NAMES.getOrDefault(
              fullSortOrderId,
              "Unknown legacy SQL collation (sort order " + fullSortOrderId + ")"
          );

          // Build detailed flag description
          StringBuilder flagDesc = new StringBuilder();
          if ((flags & 0x01) != 0) {
            flagDesc.append("CI, ");
          }
          if ((flags & 0x02) != 0) {
            flagDesc.append("AI, ");
          }
          if ((flags & 0x04) != 0) {
            flagDesc.append("WI, ");
          }
          if ((flags & 0x08) != 0) {
            flagDesc.append("KI, ");
          }
          if ((flags & 0x10) != 0) {
            flagDesc.append("BIN, ");
          }
          if ((flags & 0x20) != 0) {
            flagDesc.append("BIN2, ");
          }
          if ((flags & 0x40) != 0) {
            flagDesc.append("UTF8, ");
          }

          final String flagsStr = flagDesc.length() > 0
              ? flagDesc.substring(0, flagDesc.length() - 2)
              : "none";

          final int baseLcid = fullLcid & 0x000FFFFF; // lower 20 bits = locale ID

          // Shortened log message to satisfy LineLength check
          logger.info(
              "SQL Collation updated: {} (LCID={}, flags={}, ver={}, sort={}, id={})",
              friendlyName,
              baseLcid,
              flagsStr,
              versionNibble,
              sortNibble,
              fullSortOrderId
          );
        } else {
          logger.debug("SQL Collation updated (empty or too short: {} bytes)", newInfoLen);
        }
        break;

      case RESET_CONNECTION:
      case RESET_CONNECTION_SKIP_TRAN:
        resetToDefaults();
        logger.info("Connection reset by server ({})", type);
        break;

      case BEGIN_TRANSACTION:
      case COMMIT_TRANSACTION:
      case ROLLBACK_TRANSACTION:
        logger.debug("Transaction state changed: {}", type);
        break;

      case UNKNOWN:
        logger.warn("Received unknown ENVCHANGE type: {}", type);
        break;

      default:
        logger.debug("Unhandled ENVCHANGE type: {}", type);
        break;
    }
  }

  public SocketChannel getSocketChannel() {
    return socketChannel;
  }
}
