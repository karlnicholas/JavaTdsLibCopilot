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
  private final PacketEncoder packetEncoder;

  private TdsStreamHandler currentStreamHandler;
  private Consumer<Throwable> currentErrorHandler;

  /**
   * Primary constructor utilizing Dependency Injection.
   * Allows injecting mock connections for testing without hitting a real database.
   */
  public TdsTransport(String host, int port, ConnectionContext context,
                      NetworkConnection networkConnection,
                      PacketEncoder packetEncoder) { // FIX: Removed TdsStreamHandler from constructor
    this.host = host;
    this.port = port;
    this.context = context;
    this.networkConnection = networkConnection;

    this.tlsHandshake = new TlsHandshake();
    this.packetEncoder = packetEncoder;
  }

  /**
   * Overloaded constructor for backwards compatibility.
   */
  public TdsTransport(String host, int port, ConnectionContext context) throws IOException {
    this(host, port, context,
        new NioSocketConnection(host, port, 60_000),
        new QueryPacketBuilder()); // FIX: Removed the hardcoded null
  }

  // --- Handshake & TLS Methods ---

  public void tlsHandshake(javax.net.ssl.SSLContext sslContext) throws IOException {
    tlsHandshake.tlsHandshake(host, port, networkConnection, sslContext);
  }

  public void tlsComplete() {
    tlsHandshake.close();
  }

  public boolean isTlsActive() {
    return tlsHandshake != null && tlsHandshake.isTlsActive();
  }

  // --- Synchronous Methods (Login Phase) ---

  public void sendMessageDirect(TdsMessage tdsMessage) throws IOException {
    List<ByteBuffer> packetBuffers = packetEncoder.encodeMessage(
        tdsMessage, context.getSpid(), context.getCurrentPacketSize()
    );

    for (ByteBuffer buf : packetBuffers) {
      networkConnection.writeDirect(buf);
    }
  }

  public void sendMessageEncrypted(TdsMessage tdsMessage) throws IOException {
    List<ByteBuffer> packetBuffers = packetEncoder.encodeMessage(
        tdsMessage, context.getSpid(), context.getCurrentPacketSize()
    );

    for (ByteBuffer buffer : packetBuffers) {
      if (isTlsActive()) {
        tlsHandshake.writeEncrypted(buffer, networkConnection);
      } else {
        networkConnection.writeDirect(buffer);
      }
    }
  }

  public List<TdsMessage> receiveFullResponse() throws IOException {
    List<TdsMessage> messages = new ArrayList<>();
    TdsMessage msg;
    do {
      ByteBuffer header = ByteBuffer.allocate(TDS_HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN);
      networkConnection.readFullySync(header);
      header.flip();

      int length = Short.toUnsignedInt(header.getShort(2));
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
    packet.position(TDS_HEADER_LENGTH);
    ByteBuffer payload = packet.slice();
    return new TdsMessage(type, status, length, spid, packetId, payload, System.nanoTime(), null);
  }

  // --- Asynchronous Methods (Query Phase) ---

  public void setStreamHandlers(TdsStreamHandler streamHandler, Consumer<Throwable> errorHandler) {
    this.currentStreamHandler = streamHandler;
    this.currentErrorHandler = errorHandler;
  }

  public void enterAsyncMode() throws IOException {
    networkConnection.enterAsyncMode(context.getCurrentPacketSize());

    // FIX: Create a dynamic router lambda.
    // This allows the TdsChunkDecoder to always route bytes to the active query's decoder.
    TdsStreamHandler dynamicRouter = (payload, isEom) -> {
      if (currentStreamHandler != null) {
        currentStreamHandler.onPayloadAvailable(payload, isEom);
      } else {
        logger.warn("Received TDS payload chunk but no active stream handler is registered.");
      }
    };

    // Instantiate the decoder with the dynamic router
    TdsChunkDecoder decoder = new TdsChunkDecoder(dynamicRouter);

    networkConnection.setHandlers(
        buffer -> decoder.decode(buffer),
        error -> {
          if (currentErrorHandler != null) currentErrorHandler.accept(error);
        }
    );
  }

  // Expose the backpressure controls to the higher-level Stateful Parser
  public void suspendNetworkRead() {
    networkConnection.suspendRead();
  }

  public void resumeNetworkRead() {
    networkConnection.resumeRead();
  }

  public void sendQueryMessageAsync(TdsMessage tdsMessage) {
    List<ByteBuffer> packetBuffers = packetEncoder.encodeMessage(
        tdsMessage, context.getSpid(), context.getCurrentPacketSize()
    );
    for (ByteBuffer buf : packetBuffers) {
      networkConnection.writeAsync(buf);
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