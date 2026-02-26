package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.TdsMessage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class TdsTransport implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TdsTransport.class);

  private final SocketChannel socketChannel;
  private final String host;
  private final int port;

  // Dependencies
  private final ConnectionContext context;
  private final TlsHandshake tlsHandshake;
  private final QueryPacketBuilder packetBuilder;
  private final TdsMessageAssembler messageAssembler;

  private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean pendingWrite = new AtomicBoolean(false);
  private int readTimeoutMs = 60_000;

  private Selector selector;
  private ByteBuffer readBuffer;

  private Consumer<TdsMessage> currentMessageHandler;
  private Consumer<Throwable> currentErrorHandler;

  public TdsTransport(String host, int port, ConnectionContext context) throws IOException {
    this.host = host;
    this.port = port;
    this.context = context;

    this.tlsHandshake = new TlsHandshake();
    this.packetBuilder = new QueryPacketBuilder();
    this.messageAssembler = new TdsMessageAssembler();

    this.socketChannel = SocketChannel.open();
    this.socketChannel.configureBlocking(true);
    this.socketChannel.socket().setSoTimeout(readTimeoutMs);

    logger.trace("Initiating connection to {}:{}", host, port);
    InetSocketAddress address = new InetSocketAddress(host, port);
    if (!socketChannel.connect(address)) {
      socketChannel.finishConnect();
    }
  }

  // --- Synchronous Methods for Handshake (Added Back) ---

  public void tlsHandshake() throws IOException, NoSuchAlgorithmException, KeyManagementException {
    tlsHandshake.tlsHandshake(host, port, socketChannel);
  }

  public void tlsComplete() {
    tlsHandshake.close();
  }

  public void sendMessageDirect(TdsMessage tdsMessage) throws IOException {
    List<ByteBuffer> packetBuffers = packetBuilder.buildPackets(
        tdsMessage.getPacketType(), tdsMessage.getStatusFlags(),
        context.getSpid(), tdsMessage.getPayload(),
        (short) 1, context.getCurrentPacketSize()
    );

    for (ByteBuffer buf : packetBuffers) {
      writeDirect(buf);
    }
  }

  public void sendMessageEncrypted(TdsMessage tdsMessage) throws IOException {
    List<ByteBuffer> packetBuffers = packetBuilder.buildPackets(
        tdsMessage.getPacketType(), tdsMessage.getStatusFlags(),
        context.getSpid(), tdsMessage.getPayload(),
        (short) 1, context.getCurrentPacketSize()
    );

    for (ByteBuffer buffer : packetBuffers) {
      if (tlsHandshake.isTlsActive()) {
        tlsHandshake.writeEncrypted(buffer, socketChannel);
      } else {
        writeDirect(buffer);
      }
    }
  }

  public List<TdsMessage> receiveFullResponse() throws IOException {
    // Synchronous read loop for Login phase
    List<TdsMessage> messages = new ArrayList<>();
    TdsMessage msg;
    do {
      // Basic synchronous read wrapper
      ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
      readFullySync(header);
      header.flip();

      int length = Short.toUnsignedInt(header.getShort(2));
      ByteBuffer payload = ByteBuffer.allocate(length - 8).order(ByteOrder.LITTLE_ENDIAN);
      readFullySync(payload);
      payload.flip();

      ByteBuffer fullPacket = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
      fullPacket.put(header.array());
      fullPacket.put(payload.array());
      fullPacket.flip();

      // Use assembler to parse single packet (stateless for sync mode)
      // Note: In real sync mode we might just manually parse to keep it simple
      msg = new TdsTransport(host, port, context).buildMessageFromPacket(fullPacket);
      messages.add(msg);

    } while (!msg.isLastPacket());
    return messages;
  }

  // Helper for sync receive
  private TdsMessage buildMessageFromPacket(ByteBuffer packet) {
    packet.position(0);
    byte type = packet.get();
    byte status = packet.get();
    int length = Short.toUnsignedInt(packet.getShort());
    short spid = packet.getShort();
    byte packetId = packet.get();
    packet.position(8);
    ByteBuffer payload = packet.slice();
    return new TdsMessage(type, status, length, spid, packetId, payload, System.nanoTime(), null);
  }

  private void readFullySync(ByteBuffer buf) throws IOException {
    while (buf.hasRemaining()) {
      int read = socketChannel.read(buf);
      if (read == -1) throw new IOException("EOF during sync read");
    }
  }

  private void writeDirect(ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      socketChannel.write(buffer);
    }
  }

  public void cancelCurrent() {
    // Implementation for cancellation logic
    logger.debug("Cancel requested (not fully implemented in this refactor)");
  }

  public SocketChannel getSocketChannel() {
    return this.socketChannel;
  }

  // --- Async Methods (Existing) ---

  public void enterAsyncMode() throws IOException {
    readBuffer = ByteBuffer.allocate(context.getCurrentPacketSize());
    this.selector = Selector.open();
    socketChannel.configureBlocking(false);
    socketChannel.register(selector, SelectionKey.OP_READ, this);
    startEventLoop();
  }

  private void startEventLoop() {
    Thread eventLoopThread = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          if (selector.select(1000) == 0) continue;
          Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
          while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            if (!key.isValid()) continue;
            try {
              if (key.isReadable()) onReadable(key);
              if (key.isWritable()) onWritable(key);
            } catch (Throwable t) {
              cleanupKeyAndTransport(key);
              if (currentErrorHandler != null) currentErrorHandler.accept(t);
            }
          }
        } catch (Throwable fatal) {
          if (currentErrorHandler != null) currentErrorHandler.accept(fatal);
          break;
        }
      }
    }, "TDS-EventLoop");
    eventLoopThread.setDaemon(true);
    eventLoopThread.start();
  }

  private void onReadable(SelectionKey selectionKey) throws IOException {
    int read = socketChannel.read(readBuffer);
    if (read == -1) {
      cleanupKeyAndTransport(selectionKey);
      return;
    }
    if (read == 0) return;

    readBuffer.flip();
    try {
      messageAssembler.processNetworkBuffer(readBuffer, currentMessageHandler);
    } finally {
      readBuffer.compact();
    }
  }

  private void onWritable(SelectionKey key) throws IOException {
    while (true) {
      ByteBuffer buf = writeQueue.peek();
      if (buf == null) {
        pendingWrite.set(false);
        key.interestOpsAnd(~SelectionKey.OP_WRITE);
        return;
      }
      int written = socketChannel.write(buf);
      if (written == 0 || buf.hasRemaining()) return;
      writeQueue.poll();
    }
  }

  public void sendQueryMessageAsync(TdsMessage tdsMessage) {
    List<ByteBuffer> packetBuffers = packetBuilder.buildPackets(
        tdsMessage.getPacketType(), tdsMessage.getStatusFlags(),
        context.getSpid(), tdsMessage.getPayload(),
        (short) 1, context.getCurrentPacketSize()
    );
    for (ByteBuffer buf : packetBuffers) {
      writeAsync(buf);
    }
  }

  private void writeAsync(ByteBuffer src) {
    ByteBuffer copy = src.duplicate();
    boolean wasEmpty = writeQueue.isEmpty();
    writeQueue.offer(copy);
    if (wasEmpty && pendingWrite.compareAndSet(false, true)) {
      SelectionKey key = socketChannel.keyFor(selector);
      if (key != null && key.isValid()) {
        key.interestOpsOr(SelectionKey.OP_WRITE);
        selector.wakeup();
      }
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

  public void setClientHandlers(Consumer<TdsMessage> messageHandler, Consumer<Throwable> errorHandler) {
    this.currentMessageHandler = messageHandler;
    this.currentErrorHandler = errorHandler;
  }

  @Override
  public void close() throws IOException {
    if (socketChannel != null && socketChannel.isOpen()) {
      socketChannel.close();
    }
    if (selector != null && selector.isOpen()) {
      selector.close();
    }
  }

  public boolean isTlsActive() {
    return tlsHandshake != null && tlsHandshake.isTlsActive();
  }
}