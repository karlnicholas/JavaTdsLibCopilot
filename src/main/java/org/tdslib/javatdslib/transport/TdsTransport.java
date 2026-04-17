package org.tdslib.javatdslib.transport;

import io.r2dbc.spi.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.headers.TraceActivityHeader;
import org.tdslib.javatdslib.headers.TransactionDescriptorHeader;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.reactive.AsyncWorkerSink;
import org.tdslib.javatdslib.reactive.TdsTokenQueue;
import org.tdslib.javatdslib.tokens.StatefulTokenDecoder;
import org.tdslib.javatdslib.tokens.TokenParserRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

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
  private volatile FluxSink<Result.Segment> activeSink;

  // --- Reactive Connection Queue ---
  private final Queue<PendingRequest> requestQueue = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean isNetworkBusy = new AtomicBoolean(false);

  /**
   * Primary constructor utilizing Dependency Injection.
   * Allows injecting mock connections for testing
   * without hitting a real database.
   *
   * @param host              The hostname of the server.
   * @param port              The port number of the server.
   * @param context           The connection context.
   * @param networkConnection The network connection to use.
   * @param packetEncoder     The packet encoder to use.
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
   * @param host             The hostname of the server.
   * @param port             The port number of the server.
   * @param connectTimeoutMs The connection and read timeout in milliseconds.
   * @param context          The connection context.
   * @throws IOException If an I/O error occurs.
   */
  public TdsTransport(String host, int port, int connectTimeoutMs, ConnectionContext context)
      throws IOException {
    this(
        host,
        port,
        context,
        new NioSocketConnection(host, port, connectTimeoutMs), // Pass it dynamically here
        new QueryPacketBuilder());
  }

  // ADD THIS at the top of TdsTransport class
  public static final java.util.concurrent.ConcurrentHashMap<String, String> queryStates =
      new java.util.concurrent.ConcurrentHashMap<>();
  /**
   * Executes a TDS Message and returns a reactive stream of segments.
   * Centralizes the building of TDS headers and reactive context extraction.
   * * @param messageFactory A function that takes the constructed headers and returns a TdsMessage
   */
// UPDATE the execute method signature to accept the SQL string tracker
  public Flux<Result.Segment> execute(String sqlTracker, Function<AllHeaders, TdsMessage> messageFactory) {
    return Flux.deferContextual(contextView -> {
      UUID traceId = contextView.getOrDefault("trace-id", null);

      return Flux.create(sink -> {
        // BREADCRUMB 1: Query entered the transport queue
        queryStates.put(sqlTracker, "1_QUEUED");

        requestQueue.offer(new PendingRequest(sqlTracker, () -> {
          AllHeaders headers = buildHeaders(traceId);
          return messageFactory.apply(headers);
        }, sink));

        drain();
      });
    });
  }

  /**
   * Centralized header builder for all outgoing transport messages.
   */
  private AllHeaders buildHeaders(UUID traceId) {
    TransactionDescriptorHeader txHeader = context.isInTransaction()
        ? new TransactionDescriptorHeader(context.getTransactionDescriptor(), 1)
        : new TransactionDescriptorHeader(new byte[8], 1);

    if (traceId != null) {
      TraceActivityHeader traceHeader = new TraceActivityHeader(traceId, 1);
      return new AllHeaders(txHeader, traceHeader);
    }
    return new AllHeaders(txHeader);
  }

  /**
   * The non-blocking drain loop.
   * Ensures only one query physically writes to the socket at a time.
   */
  private void drain() {
    // 1. Acquire Lock
    if (!isNetworkBusy.compareAndSet(false, true)) {
      return;
    }

    // 2. Pop the Queue
    PendingRequest request = requestQueue.poll();
    if (request == null) {
      isNetworkBusy.set(false);
      return;
    }

    this.activeSink = request.sink();
    // Guarantee exactly-once termination and handoff
    AtomicBoolean isFinished = new AtomicBoolean(false);

    try {
// UPDATE THIS LINE to pass the sqlTracker down to the sink
      TdsTokenQueue tokenQueue = new TdsTokenQueue(this);
      AsyncWorkerSink workerSink = new AsyncWorkerSink(request.sqlTracker(), tokenQueue, context, Schedulers.parallel());

      logger.trace("[RACE-TRACE] >>> Starting execution for NEW query from queue.");

      // Wire Callbacks - ONLY the first terminal signal triggers the handoff
      workerSink.setCallbacks(
          request.sink()::next,
          error -> {
            if (isFinished.compareAndSet(false, true)) {
              logger.trace(
                  "[RACE-TRACE] 🔴 workerSink.onError triggered. Releasing lock and draining!");
              this.setStreamHandlers(null);
              this.resumeNetworkRead();
              request.sink().error(error);
              isNetworkBusy.set(false);
              drain();
            }
          },
          () -> {
            if (isFinished.compareAndSet(false, true)) {
              logger.trace(
                  "[RACE-TRACE] 🟢 workerSink.onComplete triggered. Releasing lock and draining!");
              this.setStreamHandlers(null);
              this.resumeNetworkRead();
              request.sink().complete();
              isNetworkBusy.set(false);
              drain();
            }
          }
      );

      request.sink().onRequest(workerSink::request);
      request.sink().onCancel(() -> {
        workerSink.cancel();
        if (isFinished.compareAndSet(false, true)) {
          logger.trace(
              "[RACE-TRACE] 🟡 Downstream onCancel triggered! Releasing lock and draining!");
          this.setStreamHandlers(null);
          this.resumeNetworkRead();
          isNetworkBusy.set(false);
          drain();
        }
      });

      StatefulTokenDecoder decoder = new StatefulTokenDecoder(
          TokenParserRegistry.DEFAULT, context, tokenQueue);

      logger.trace(
          "[RACE-TRACE] 🔵 Registering new StatefulTokenDecoder to the network pipeline.");
      this.setStreamHandlers(decoder::onPayloadAvailable);

      // ADD BREADCRUMB 2 right before sending the bytes
      queryStates.put(request.sqlTracker(), "2_SENT_TO_DB");

      TdsMessage message = request.messageSupplier().get();
      this.sendQueryMessageAsync(message);

    } catch (Exception e) {
      if (isFinished.compareAndSet(false, true)) {
        logger.error(
            "[RACE-TRACE] 💥 Exception during setup/supplier eval. Releasing lock and draining!", e);
        this.setStreamHandlers(null);
        request.sink().error(e);
        isNetworkBusy.set(false);
        drain();
      }
    }
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

  /**
   * Completes the TLS handshake and cleans up resources.
   */
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
    List<ByteBuffer> packetBuffers = packetEncoder.encodeMessage(
        tdsMessage, context.getSpid(), context.getCurrentPacketSize());

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
    List<ByteBuffer> packetBuffers = packetEncoder.encodeMessage(
        tdsMessage, context.getSpid(), context.getCurrentPacketSize());

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
    PacketType type = PacketType.valueOf(packet.get());
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
   */
  public void setStreamHandlers(TdsStreamHandler streamHandler) {
    this.currentStreamHandler = streamHandler;
  }

  /**
   * Enters asynchronous mode for the transport.
   *
   * @throws IOException If an I/O error occurs.
   */
  public void enterAsyncMode() throws IOException {
    networkConnection.enterAsyncMode(context.getCurrentPacketSize());

    TdsStreamHandler dynamicRouter =
        (payload, isEom) -> {
          if (currentStreamHandler != null) {
            currentStreamHandler.onPayloadAvailable(payload, isEom);
          } else {
            // NEW: Instantly trigger a fatal shutdown if bytes arrive without a handler
            handleFatalConnectionError(new IllegalStateException(
                "Protocol Desync: Received TDS payload chunk "
                    + "but no active stream handler is registered."));
          }
        };

    TdsPacketFramer decoder = new TdsPacketFramer(dynamicRouter);

    networkConnection.setHandlers(
        decoder::decode,
        this::handleFatalConnectionError);
  }

  // Expose the backpressure controls to the higher-level Stateful Parser

  /**
   * Suspends reading from the network.
   */
  public void suspendNetworkRead() {
    logger.trace("[TdsTransport] Propagating suspendNetworkRead() to NioSocketConnection.");
    networkConnection.suspendRead();
  }

  /**
   * Resumes reading from the network.
   */
  public void resumeNetworkRead() {
    logger.trace("[TdsTransport] Propagating resumeNetworkRead() to NioSocketConnection.");
    networkConnection.resumeRead();
  }

  /**
   * Sends a query message asynchronously.
   *
   * @param tdsMessage The message to send.
   */
  public void sendQueryMessageAsync(TdsMessage tdsMessage) {
    List<ByteBuffer> packetBuffers = packetEncoder.encodeMessage(
        tdsMessage, context.getSpid(), context.getCurrentPacketSize());
    for (ByteBuffer buf : packetBuffers) {
      networkConnection.writeAsync(buf);
    }
  }

  /**
   * Cancels the current operation.
   */
  public void cancelCurrent() {
    logger.debug("Cancel requested");
  }

  @Override
  public void close() throws IOException {
    if (networkConnection != null) {
      networkConnection.close();
    }
  }

  private void handleFatalConnectionError(Throwable error) {
    logger.error("Fatal connection error. Aborting active query and draining queue.", error);

    // 1. Close the physical connection so no more bytes arrive
    try {
      close();
    } catch (Exception ignored) {
      // Ignored
    }

    // 2. Send the error directly to the active sink, bypassing standard handlers
    if (this.activeSink != null) {
      this.activeSink.error(error);
      this.activeSink = null;
    }

    // 3. Flush the queue and fail any pending queries
    PendingRequest pending;
    while ((pending = requestQueue.poll()) != null) {
      pending.sink().error(error);
    }

    isNetworkBusy.set(false);
  }

  /**
   * Holds the late-binding message recipe and the reactive sink for a queued request.
   */
  private record PendingRequest(
      String sqlTracker,
      Supplier<TdsMessage> messageSupplier,
      FluxSink<Result.Segment> sink
  ) {
  }
}
