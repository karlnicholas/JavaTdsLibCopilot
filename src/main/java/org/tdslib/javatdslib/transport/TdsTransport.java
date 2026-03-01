package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.TdsMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Orchestrates the TDS protocol layer.
 * Delegates all physical I/O to the injected NetworkConnection.
 */
public class TdsTransport implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TdsTransport.class);
  private static final int TDS_HEADER_LENGTH = 8;

  private final NetworkConnection networkConnection;
  private final String host;
  private final int port;

  // Dependencies
  private final ConnectionContext context;
  private final TlsHandshake tlsHandshake;
  private final QueryPacketBuilder packetBuilder;
  private final PacketAssembler messageAssembler;

  private Consumer<TdsMessage> currentMessageHandler;
  private Consumer<Throwable> currentErrorHandler;

  /**
   * Primary constructor utilizing Dependency Injection.
   * Allows injecting mock connections for testing without hitting a real database.
   */
  public TdsTransport(String host, int port, ConnectionContext context, NetworkConnection networkConnection) {
    this.host = host;
    this.port = port;
    this.context = context;
    this.networkConnection = networkConnection;

    this.tlsHandshake = new TlsHandshake();
    this.packetBuilder = new QueryPacketBuilder();
    this.messageAssembler = new PacketAssembler();
  }

  /**
   * Overloaded constructor for backwards compatibility.
   */
  public TdsTransport(String host, int port, ConnectionContext context) throws IOException {
    this(host, port, context, new NioSocketConnection(host, port, 60_000));
  }

  // --- Handshake & TLS Methods ---

  public void tlsHandshake(javax.net.ssl.SSLContext sslContext) throws IOException {
    // Exposing the socket channel specifically for the TLS handshake phase
    tlsHandshake.tlsHandshake(host, port, networkConnection.getSocketChannel(), sslContext);
  }

  public void tlsComplete() {
    tlsHandshake.close();
  }

  public boolean isTlsActive() {
    return tlsHandshake != null && tlsHandshake.isTlsActive();
  }

  // --- Synchronous Methods (Login Phase) ---

  public void sendMessageDirect(TdsMessage tdsMessage) throws IOException {
    List<ByteBuffer> packetBuffers = packetBuilder.buildPackets(
        tdsMessage.getPacketType(), tdsMessage.getStatusFlags(),
        context.getSpid(), tdsMessage.getPayload(),
        (short) 1, context.getCurrentPacketSize()
    );

    for (ByteBuffer buf : packetBuffers) {
      networkConnection.writeDirect(buf);
    }
  }

  public void sendMessageEncrypted(TdsMessage tdsMessage) throws IOException {
    List<ByteBuffer> packetBuffers = packetBuilder.buildPackets(
        tdsMessage.getPacketType(), tdsMessage.getStatusFlags(),
        context.getSpid(), tdsMessage.getPayload(),
        (short) 1, context.getCurrentPacketSize()
    );

    for (ByteBuffer buffer : packetBuffers) {
      if (isTlsActive()) {
        tlsHandshake.writeEncrypted(buffer, networkConnection.getSocketChannel());
      } else {
        networkConnection.writeDirect(buffer);
      }
    }
  }

  public List<TdsMessage> receiveFullResponse() throws IOException {
    List<TdsMessage> messages = new ArrayList<>();
    TdsMessage msg;
    do {
      // 1. Replaced magic number 8 with TDS_HEADER_LENGTH
      ByteBuffer header = ByteBuffer.allocate(TDS_HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN);
      networkConnection.readFullySync(header);
      header.flip();

      int length = Short.toUnsignedInt(header.getShort(2));
      // 2. Replaced magic number 8 with TDS_HEADER_LENGTH
      ByteBuffer payload = ByteBuffer.allocate(length - TDS_HEADER_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
      networkConnection.readFullySync(payload);
      payload.flip();

      ByteBuffer fullPacket = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
      fullPacket.put(header.array());
      fullPacket.put(payload.array());
      fullPacket.flip();

      msg = buildMessageFromPacket(fullPacket);
      messages.add(msg);

    } while (!msg.isLastPacket());

    return messages;
  }

  private TdsMessage buildMessageFromPacket(ByteBuffer packet) {
    packet.position(0);
    byte type = packet.get();
    byte status = packet.get();
    int length = Short.toUnsignedInt(packet.getShort());
    short spid = packet.getShort();
    byte packetId = packet.get();
    packet.position(TDS_HEADER_LENGTH); // 3. Replaced magic number 8
    ByteBuffer payload = packet.slice();
    return new TdsMessage(type, status, length, spid, packetId, payload, System.nanoTime(), null);
  }

  // --- Asynchronous Methods (Query Phase) ---

  public void setClientHandlers(Consumer<TdsMessage> messageHandler, Consumer<Throwable> errorHandler) {
    this.currentMessageHandler = messageHandler;
    this.currentErrorHandler = errorHandler;
  }

  public void enterAsyncMode() throws IOException {
    networkConnection.enterAsyncMode(context.getCurrentPacketSize());

    // Wire the network callback directly into our TDS Assembler
    networkConnection.setHandlers(
        buffer -> messageAssembler.processNetworkBuffer(buffer, currentMessageHandler),
        error -> {
          if (currentErrorHandler != null) currentErrorHandler.accept(error);
        }
    );
  }

  public void sendQueryMessageAsync(TdsMessage tdsMessage) {
    List<ByteBuffer> packetBuffers = packetBuilder.buildPackets(
        tdsMessage.getPacketType(), tdsMessage.getStatusFlags(),
        context.getSpid(), tdsMessage.getPayload(),
        (short) 1, context.getCurrentPacketSize()
    );
    for (ByteBuffer buf : packetBuffers) {
      networkConnection.writeAsync(buf); // Fire and forget to the event loop
    }
  }

  public void cancelCurrent() {
    logger.debug("Cancel requested");
  }

  @Override
  public void close() throws IOException {
    if (networkConnection != null) {
      networkConnection.close();
    }
  }
}