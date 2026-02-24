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
import java.util.*;
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
  private static final int STATUS_OFFSET = 1;
  private static final int EOM_BIT = 0x01;

  private int readTimeoutMs = 60_000;

  private Selector selector;           // will be set when entering async mode

  // ── Read state (packet framing) ───────────────────────
  private ByteBuffer readBuffer;

  // ── Aggregation state for multi-packet messages ───────
  private final List<byte[]> inboundMessageParts = new ArrayList<>();
  private int inboundMessageTotalLength = 0;
  private byte inboundMessageType = 0;
  private short inboundMessageSpid = 0;
  private byte inboundMessagePacketId = 0;

  private Consumer<TdsMessage> currentMessageHandler;   // callback from TdsConnection
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
            } catch (Throwable t) {
              logger.error("Error processing transport {}", transport, t);
              cleanupKeyAndTransport(key);
              currentErrorHandler.accept(t);
            }
          }
        } catch (Throwable fatal) {
          logger.error("Fatal error in TDS event loop", fatal);
          currentErrorHandler.accept(fatal);
          break;
        }
      }
    }, "TDS-EventLoop-" + host + ":" + port);

    eventLoopThread.setDaemon(true);
    eventLoopThread.start();
  }

  private void cleanupKeyAndTransport(SelectionKey key) {
    try {
      key.cancel();
      close();
    } catch (Exception e) {
      logger.warn("Failed to clean up key/transport", e);
    }
  }

  private void onWritable(SelectionKey key) throws IOException {
    SocketChannel ch = (SocketChannel) key.channel();

    while (true) {
      ByteBuffer buf = writeQueue.peek();
      if (buf == null) {
        pendingWrite.set(false);
        key.interestOpsAnd(~SelectionKey.OP_WRITE);
        return;
      }

      int written = ch.write(buf);
      if (written == 0) return;
      if (buf.hasRemaining()) return;

      writeQueue.poll();
    }
  }

  /**
   * Handle readable selector events: read available bytes, assemble full TDS packets,
   * aggregate packets into logical TDS Messages (handling packet fragmentation),
   * and deliver to the handler.
   *
   * @throws IOException on I/O error while reading from the socket
   */
  public void onReadable(SelectionKey selectionKey) throws IOException {
    // 1. Read once
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
        // Peek at length (bytes 2-3)
        int length = Short.toUnsignedInt(readBuffer.getShort(readBuffer.position() + 2));

        // Safety Check
        if (length > readBuffer.capacity()) {
          throw new IOException("TDS Packet length (" + length + ") exceeds buffer capacity");
        }

        if (readBuffer.remaining() < length) {
          break; // Wait for more data
        }

        // --- Process Header ---
        int packetStart = readBuffer.position();
        byte type = readBuffer.get(packetStart);
        byte status = readBuffer.get(packetStart + STATUS_OFFSET);
        short spid = readBuffer.getShort(packetStart + 4);
        byte packetId = readBuffer.get(packetStart + 6);

        // --- Extract Payload ---
        // We MUST copy the payload because readBuffer will be compacted (reused)
        int payloadLen = length - TDS_HEADER_LENGTH;
        byte[] payloadChunk = new byte[payloadLen];

        // Move position to payload start
        readBuffer.position(packetStart + TDS_HEADER_LENGTH);
        readBuffer.get(payloadChunk);

        // Current readBuffer position is now at the end of this packet
        // (packetStart + 8 + payloadLen == packetStart + length)

        // --- Aggregation Logic ---
        if (inboundMessageParts.isEmpty()) {
          inboundMessageType = type;
          inboundMessageSpid = spid;
          inboundMessagePacketId = packetId;
        } else {
          // Optional: Validate type consistency (should match inboundMessageType)
        }

        inboundMessageParts.add(payloadChunk);
        inboundMessageTotalLength += payloadLen;

        // --- Check EOM (End of Message) ---
        if ((status & EOM_BIT) != 0) {
          // Reassemble full logical message
          ByteBuffer fullPayload = ByteBuffer.allocate(inboundMessageTotalLength)
                  .order(ByteOrder.LITTLE_ENDIAN);

          for (byte[] part : inboundMessageParts) {
            fullPayload.put(part);
          }
          fullPayload.flip();

          TdsMessage logicalMessage = new TdsMessage(
                  inboundMessageType,
                  status,
                  inboundMessageTotalLength + TDS_HEADER_LENGTH, // Logical total length
                  inboundMessageSpid,
                  inboundMessagePacketId,
                  fullPayload,
                  System.nanoTime(),
                  null
          );

          // Reset aggregation state
          inboundMessageParts.clear();
          inboundMessageTotalLength = 0;

          // Dispatch complete message
          currentMessageHandler.accept(logicalMessage);
        }
      }
    } finally {
      readBuffer.compact();
    }
  }

  /**
   * Update read timeout for the underlying socket.
   */
  public void setReadTimeout(final int ms) throws SocketException {
    this.readTimeoutMs = ms;
    socketChannel.socket().setSoTimeout(ms);
  }

  /**
   * Receives a complete logical response by reading packets until EOM.
   * Used for SYNCHRONOUS mode (e.g. Login).
   */
  public List<TdsMessage> receiveFullResponse() throws IOException {
    List<TdsMessage> tdsMessages = new ArrayList<>();
    TdsMessage packet;
    do {
      packet = receiveSinglePacket();
      tdsMessages.add(packet);
    } while (!packet.isLastPacket());
    return tdsMessages;
  }

  public TdsMessage buildMessageFromPacket(ByteBuffer packet) {
    // Legacy helper - primarily for synchronous reads if needed
    packet.mark();
    final byte type   = packet.get();
    final byte status = packet.get();
    final int length  = Short.toUnsignedInt(packet.getShort());
    final short spid  = packet.getShort();
    final byte packetId = packet.get();
    final byte window = packet.get();

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
            null
    );
  }

  /**
   * Receives ONE single TDS packet. Used for Synchronous Mode.
   */
  public TdsMessage receiveSinglePacket() throws IOException {
    ByteBuffer rawPacket = readRawPacket();
    return buildMessageFromPacket(rawPacket);
  }

  public ByteBuffer readRawPacket() throws IOException {
    ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    readFully(header);
    header.flip();

    byte packetType = header.get();
    byte status = header.get();
    int length = header.getShort() & 0xFFFF;

    if (length < 8 || length > 32767) {
      throw new IOException("Invalid TDS packet length: " + length);
    }

    ByteBuffer payloadBuffer = ByteBuffer.allocate(length - 8).order(ByteOrder.LITTLE_ENDIAN);
    readFully(payloadBuffer);
    payloadBuffer.flip();

    ByteBuffer fullPacket = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
    fullPacket.put(header.array());
    fullPacket.put(payloadBuffer.array());
    fullPacket.flip();

    return fullPacket;
  }

  public void readFully(final ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      final int read = socketChannel.read(buffer);
      if (read == -1) {
        throw new IOException("Unexpected end of stream");
      }
    }
  }

  public void sendMessageDirect(TdsMessage tdsMessage) throws IOException {
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

    for (ByteBuffer buf : packetBuffers) {
      writeDirect(buf);
    }
  }

  public void setClientHandlers(
          Consumer<TdsMessage> currentMessageHandler,
          Consumer<Throwable> currentErrorHandler) {
    this.currentMessageHandler = currentMessageHandler;
    this.currentErrorHandler = currentErrorHandler;
  }

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

    for (ByteBuffer buf : packetBuffers) {
      writeAsync(buf);
    }
  }

  public void writeDirect(ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      socketChannel.write(buffer);
    }
  }

  public void writeAsync(ByteBuffer src) {
    SelectionKey key;
    ByteBuffer copy = src.duplicate();

    boolean wasEmpty = writeQueue.isEmpty();
    writeQueue.offer(copy);

    if (wasEmpty && pendingWrite.compareAndSet(false, true)) {
      key = socketChannel.keyFor(selector);
      if (key != null && key.isValid()) {
        key.interestOpsOr(SelectionKey.OP_WRITE);
        selector.wakeup();
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (socketChannel != null && socketChannel.isOpen()) {
      // This sends the TCP FIN packet to SQL Server
      socketChannel.close();
      logger.info("Connection to SQL Server gracefully closed.");
    }
    // Also close your NIO Selector if you have an event loop running
    // if (selector != null && selector.isOpen()) {
    //   selector.close();
    // }
  }

  private String logHex(ByteBuffer buffer) {
    StringBuilder sb = new StringBuilder();
    sb.append(" (Length: ").append(buffer.remaining()).append(")\n");
    int pos = buffer.position();
    int i = 0;
    while (buffer.hasRemaining()) {
      byte b = buffer.get();
      sb.append(String.format("%02X ", b));
      if (++i % 16 == 0) sb.append("\n");
    }
    sb.append("\n");
    buffer.position(pos);
    return sb.toString();
  }

  public void tlsHandshake() throws IOException, NoSuchAlgorithmException, KeyManagementException {
    tlsHandshake.tlsHandshake(host, port, socketChannel);
  }

  public boolean isTlsActive() {
    return tlsHandshake != null && tlsHandshake.isTlsActive();
  }

  public void tlsComplete() {
    // Drop the TLS engine abruptly (No close_notify sent)
    if (tlsHandshake != null) {
      tlsHandshake.close();
    }
  }

  public void sendMessageEncrypted(TdsMessage tdsMessage) throws IOException {
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

    for (ByteBuffer buffer : packetBuffers) {
      if (isTlsActive()) {
        tlsHandshake.writeEncrypted(buffer, socketChannel);
      } else {
        // Safe fallback if the server rejected TLS entirely
        writeDirect(buffer);
      }
    }
  }

  public void cancelCurrent() {}

  @Override public TdsVersion getTdsVersion() { return tdsVersion; }
  @Override public void setTdsVersion(TdsVersion version) { this.tdsVersion = version; }
  @Override public boolean isUnicodeEnabled() { return tdsVersion.ordinal() >= TdsVersion.V7_1.ordinal(); }
  @Override public String getCurrentDatabase() { return currentDatabase; }
  @Override public void setDatabase(String database) { this.currentDatabase = database; }
  @Override public String getCurrentLanguage() { return currentLanguage; }
  @Override public void setLanguage(String language) { this.currentLanguage = language; }
  @Override public String getCurrentCharset() { return currentCharset; }
  @Override public void setCharset(String charset) { this.currentCharset = charset; }
  @Override public int getCurrentPacketSize() { return packetSize; }
  @Override public void setPacketSize(int size) { this.packetSize = size; }
  @Override public byte[] getCurrentCollationBytes() { return Arrays.copyOf(currentCollationBytes, currentCollationBytes.length); }
  @Override public boolean isInTransaction() { return inTransaction; }
  @Override public void setInTransaction(boolean inTransaction) { this.inTransaction = inTransaction; }
  @Override public String getServerName() { return serverName; }
  @Override public void setServerName(String serverName) { this.serverName = serverName; }
  @Override public String getServerVersionString() { return serverVersionString; }
  @Override public void setServerVersionString(String versionString) { this.serverVersionString = versionString; }
  @Override public int getSpid() { return spid; }
  @Override public void setSpid(int spid) { logger.debug("Setting SPID to {}", spid); this.spid = spid; }
  @Override public Optional<Charset> getNonUnicodeCharset() { return CollationUtils.getCharsetFromCollation(currentCollationBytes); }
  @Override public void setCollationBytes(byte[] collationBytes) { this.currentCollationBytes = collationBytes != null ? collationBytes.clone() : null; }

  @Override
  public void resetToDefaults() {
    currentDatabase = null;
    currentLanguage = "us_english";
    currentCharset = null;
    packetSize = 4096;
    currentCollationBytes = new byte[0];
    inTransaction = false;
    spid = 0;
    logger.debug("Session state reset due to resetConnection flag");
  }

  public void applyEnvChange(EnvChangeToken change) {
    EnvChangeType type = change.getChangeType();
    Charset charset = isUnicodeEnabled() ? StandardCharsets.UTF_16LE : StandardCharsets.US_ASCII;
    ByteBuffer buf = ByteBuffer.wrap(change.getValueBytes());

    switch (type) {
      case DATABASE:
      case LANGUAGE:
      case CHARSET:
        int dbNewCharLen = buf.get() & 0xFF;
        byte[] dbNewData = new byte[dbNewCharLen * 2];
        buf.get(dbNewData);
        String dbNewValue = new String(dbNewData, charset).trim();
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

      default:
        logger.debug("Unhandled ENVCHANGE type: {}", type);
        break;
    }
  }

  public SocketChannel getSocketChannel() {
    return socketChannel;
  }
}