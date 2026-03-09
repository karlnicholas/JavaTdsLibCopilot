package org.tdslib.javatdslib.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.TdsMessage;

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
   * Allows injecting mock connections for testing
   * without hitting a real database.
   *
   * @param host The hostname of the server.
   * @param port The port number of the server.
   * @param context The connection context.
   * @param networkConnection The network connection to use.
   * @param packetEncoder The packet encoder to use.
   */
  public TdsTransport(
      String host,
      int port,
      ConnectionContext context,
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
   *
   * @param host The hostname of the server.
   * @param port The port number of the server.
   * @param context The connection context.
   * @throws IOException If an I/O error occurs.
   */
  public TdsTransport(String host, int port, ConnectionContext context) throws IOException {
    this(
        host,
        port,
        context,
        new NioSocketConnection(host, port, 60_000),
        new QueryPacketBuilder()); // FIX: Removed the hardcoded null
  }

  // --- Handshake & TLS Methods ---

  /**
   * Performs the TLS handshake.
   *
   * @param sslContext The SSL context to use.
   * @throws IOException If an I/O error occurs.
   */
  public void tlsHandshake(javax.net.ssl.SSLContext sslContext) throws IOException {
    tlsHandshake.tlsHandshake(host, port, networkConnection, sslContext);
  }

  /** Completes the TLS handshake and cleans up resources. */
  public void tlsComplete() {
    tlsHandshake.close();
  }

  /**
   * Checks if TLS is currently active.
   *
   * @return true if TLS is active, false otherwise.
   */
  public boolean isTlsActive() {
    return tlsHandshake != null && tlsHandshake.isTlsActive();
  }

  // --- Synchronous Methods (Login Phase) ---

  /**
   * Sends a TDS message directly to the network without encryption.
   *
   * @param tdsMessage The message to send.
   * @throws IOException If an I/O error occurs.
   */
  public void sendMessageDirect(TdsMessage tdsMessage) throws IOException {
    List<ByteBuffer> packetBuffers =
        packetEncoder.encodeMessage(tdsMessage, context.getSpid(), context.getCurrentPacketSize());

    for (ByteBuffer buf : packetBuffers) {
      networkConnection.writeDirect(buf);
    }
  }

  /**
   * Sends a TDS message, encrypting it if TLS is active.
   *
   * @param tdsMessage The message to send.
   * @throws IOException If an I/O error occurs.
   */
  public void sendMessageEncrypted(TdsMessage tdsMessage) throws IOException {
    List<ByteBuffer> packetBuffers =
        packetEncoder.encodeMessage(tdsMessage, context.getSpid(), context.getCurrentPacketSize());

    for (ByteBuffer buffer : packetBuffers) {
      if (isTlsActive()) {
        tlsHandshake.writeEncrypted(buffer, networkConnection);
      } else {
        networkConnection.writeDirect(buffer);
      }
    }
  }

  /**
   * Receives a full TDS response synchronously.
   *
   * @return A list of TDS messages received.
   * @throws IOException If an I/O error occurs.
   */
  public List<TdsMessage> receiveFullResponse() throws IOException {
    List<TdsMessage> messages = new ArrayList<>();
    TdsMessage msg;
    do {
      ByteBuffer header = ByteBuffer.allocate(TDS_HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN);
      networkConnection.readFullySync(header);
      header.flip();

      int length = Short.toUnsignedInt(header.getShort(2));
      ByteBuffer payload =
          ByteBuffer.allocate(length - TDS_HEADER_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
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

  /**
   * Sets the handlers for asynchronous stream processing.
   *
   * @param streamHandler The handler for stream data.
   * @param errorHandler The handler for errors.
   */
  public void setStreamHandlers(TdsStreamHandler streamHandler, Consumer<Throwable> errorHandler) {
    this.currentStreamHandler = streamHandler;
    this.currentErrorHandler = errorHandler;
  }

  /**
   * Enters asynchronous mode for the transport.
   *
   * @throws IOException If an I/O error occurs.
   */
  public void enterAsyncMode() throws IOException {
    networkConnection.enterAsyncMode(context.getCurrentPacketSize());

    // FIX: Create a dynamic router lambda.
    // This allows the TdsChunkDecoder to always route bytes to the active query's decoder.
    TdsStreamHandler dynamicRouter =
        (payload, isEom) -> {
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
          if (currentErrorHandler != null) {
            currentErrorHandler.accept(error);
          }
        });
  }

  // Expose the backpressure controls to the higher-level Stateful Parser

  /** Suspends reading from the network. */
  public void suspendNetworkRead() {
    logger.trace("[TdsTransport] Propagating suspendNetworkRead() to NioSocketConnection.");
    networkConnection.suspendRead();
  }

  /** Resumes reading from the network. */
  public void resumeNetworkRead() {
    logger.trace("[TdsTransport] Propagating resumeNetworkRead() to NioSocketConnection.");
    networkConnection.resumeRead();
  }

  /** * Dynamically hot-swaps the active receiver of network bytes.
   * Used to route traffic directly to LOB streams bypassing the token parser.
   */
  public void switchStreamHandler(org.tdslib.javatdslib.transport.TdsStreamHandler newHandler) {
    if (newHandler != null) {
      org.slf4j.LoggerFactory.getLogger(TdsTransport.class)
          .trace("[TdsTransport] Dynamically switching active stream handler to: {}", newHandler.getClass().getSimpleName());
      this.currentStreamHandler = newHandler;
    }
  }

  /**
   * Sends a query message asynchronously.
   *
   * @param tdsMessage The message to send.
   */
  public void sendQueryMessageAsync(TdsMessage tdsMessage) {
    List<ByteBuffer> packetBuffers =
        packetEncoder.encodeMessage(tdsMessage, context.getSpid(), context.getCurrentPacketSize());
    for (ByteBuffer buf : packetBuffers) {
      networkConnection.writeAsync(buf);
    }
  }

  /** Cancels the current operation. */
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
