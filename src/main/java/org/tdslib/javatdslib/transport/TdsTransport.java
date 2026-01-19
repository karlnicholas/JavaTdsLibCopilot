package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.TdsMessage;

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
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Low-level TCP transport for TDS communication.
 * Supports both plain TCP and TLS (SQL Server encrypted connection).
 */
public class TdsTransport implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TdsTransport.class);

  private final SocketChannel socketChannel;
  private final String host;
  private final int port;
  private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean pendingWrite = new AtomicBoolean(false);

  private static final int TDS_HEADER_LENGTH = 8;

  private int readTimeoutMs = 60_000;
  private int packetSize = 4096;  // Default TDS packet size, updated via ENVCHANGE

  private Selector selector;           // will be set when entering async mode

  // ── Read state (packet framing) ───────────────────────
  private ByteBuffer readBuffer = ByteBuffer.allocate(packetSize);

  private final Consumer<TdsMessage> messageHandler;   // callback from TdsClient
  private final Consumer<Throwable> errorHandler;  // passed by library user
  private final TlsHandshake tlsHandshake;

  /**
   * Opens a new TCP connection to the given host and port.
   *
   * @param host remote hostname
   * @param port remote port
   * @throws IOException on I/O error while opening the socket
   */
  public TdsTransport(
      final String host,
      final int port,
      Consumer<TdsMessage> messageHandler,
      Consumer<Throwable> errorHandler
  ) throws IOException {
    this.host = host;
    this.port = port;
    this.messageHandler = messageHandler;
    this.errorHandler = errorHandler;
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
        messageHandler.accept(tdsMessage);
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
    // If the tdsMessage payload is small, send as single packet
    logger.trace("Sending tdsMessage {}", logHex(tdsMessage.getPayload()));
    // If large, split into multiple packets (max ~4096 bytes each)
    List<ByteBuffer> packetBuffers = buildPackets(
        tdsMessage.getPacketType(),
        tdsMessage.getStatusFlags(),
        tdsMessage.getSpid(),
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
   * Sends a complete logical tdsMessage (may be split into multiple packets) asynchronously.
   *
   * @param tdsMessage the tdsMessage to send (usually built by the client layer)
   * @throws IOException if sending fails
   */
  public void sendMessageAsync(TdsMessage tdsMessage) throws IOException {
    // If the tdsMessage payload is small, send as single packet
    logger.trace("Sending tdsMessage {}", logHex(tdsMessage.getPayload()));
    // If large, split into multiple packets (max ~4096 bytes each)
    List<ByteBuffer> packetBuffers = buildPackets(
        tdsMessage.getPacketType(),
        tdsMessage.getStatusFlags(),
        tdsMessage.getSpid(),
        tdsMessage.getPayload(),
        (short) 1,
        getCurrentPacketSize()
    );

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
   * @throws IOException on I/O error
   */
  public void writeAsync(ByteBuffer src) throws IOException {
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

  private TdsMessage buildMessageFromPacket(ByteBuffer packet) {
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
   * @throws IOException if closing handshake resources fails
   */
  public void tlsComplete() throws IOException {
    tlsHandshake.close();
  }

  /**
   * Sends a logical TDS tdsMessage over the established TLS session.
   *
   * <p>The tdsMessage is split into one or more TDS packets via
   * {@link #buildPackets(byte, byte, int, ByteBuffer, short, int)}
   * using the current packet size ({@link #getCurrentPacketSize()}).
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

    logger.trace("Sending tdsMessage {}", logHex(tdsMessage.getPayload()));
    // If large, split into multiple packets (max ~4096 bytes each)
    List<ByteBuffer> packetBuffers = buildPackets(
        tdsMessage.getPacketType(),
        tdsMessage.getStatusFlags(),
        tdsMessage.getSpid(),
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

}
