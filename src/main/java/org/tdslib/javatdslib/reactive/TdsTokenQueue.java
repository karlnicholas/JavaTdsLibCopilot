package org.tdslib.javatdslib.reactive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
import org.tdslib.javatdslib.reactive.events.ErrorEvent;
import org.tdslib.javatdslib.reactive.events.TdsStreamEvent;
import org.tdslib.javatdslib.reactive.events.TokenEvent;
import org.tdslib.javatdslib.tokens.ColumnData;
import org.tdslib.javatdslib.tokens.TdsDecoderSink;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A strictly bounded concurrent queue.
 * Responsible ONLY for buffering events and managing network backpressure (Watermarks).
 */
public class TdsTokenQueue implements TdsDecoderSink {
  private static final Logger logger = LoggerFactory.getLogger(TdsTokenQueue.class);

  private static final int HIGH_WATERMARK = 5 * 1024 * 1024; // 5 MB
  private static final int LOW_WATERMARK = 1024 * 1024;  // 1 MB

  private final TdsTransport transport;
  private final Queue<TdsStreamEvent> queue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger queueByteWeight = new AtomicInteger(0);
  private final AtomicBoolean isNetworkSuspended = new AtomicBoolean(false);

  private Runnable onEventAvailableCallback;

  /**
   * Constructs a new TdsTokenQueue.
   *
   * @param transport The TDS transport.
   */
  public TdsTokenQueue(TdsTransport transport) {
    this.transport = transport;
  }

  /**
   * Sets the callback to invoke when an event is available.
   *
   * @param callback The callback to invoke.
   */
  public void setOnEventAvailableCallback(Runnable callback) {
    this.onEventAvailableCallback = callback;
  }

  // ====================================================================================
  // PRODUCER: NETWORK THREAD (StatefulTokenDecoder pushes here)
  // ====================================================================================

  @Override
  public void onToken(Token token) {
    offer(new TokenEvent(token));
  }

  @Override
  public void onColumnData(ColumnData data) {
    offer(new ColumnEvent(data));
  }

  @Override
  public void onError(Throwable error) {
    offer(new ErrorEvent(error));
  }

  private void offer(TdsStreamEvent event) {
    // TODO: check offer, paranoid code.
    queue.offer(event);
    int currentWeight = queueByteWeight.addAndGet(event.getByteWeight());
    logger.trace("Enqueued {}. Current weight: {} bytes",
        event.getClass().getSimpleName(), currentWeight);

    // 1. Manage High Watermark (Suspend)
    if (currentWeight > HIGH_WATERMARK && isNetworkSuspended.compareAndSet(false, true)) {
      logger.debug(
          "HIGH WATERMARK BREACHED ({} bytes). Triggering network suspension.",
          currentWeight);
      transport.suspendNetworkRead();
    }

    // 2. Wake up the consumer
    if (onEventAvailableCallback != null) {
      onEventAvailableCallback.run();
    }
  }

  // ====================================================================================
  // CONSUMER: WORKER THREAD (AsyncWorkerSink pulls from here)
  // ====================================================================================

  /**
   * Polls the next event from the queue.
   *
   * @return The next event, or null if the queue is empty.
   */
  public TdsStreamEvent poll() {
    TdsStreamEvent event = queue.poll();
    if (event != null) {
      int weight = queueByteWeight.addAndGet(-event.getByteWeight());
      logger.trace("Dequeued {}. Current weight: {} bytes",
          event.getClass().getSimpleName(), weight);

      // Manage Low Watermark (Resume)
      if (weight < LOW_WATERMARK && isNetworkSuspended.compareAndSet(true, false)) {
        logger.debug("LOW WATERMARK REACHED ({} bytes). Triggering network resumption.",
            weight);
        transport.resumeNetworkRead();
      }
    }
    return event;
  }

  /**
   * Peeks the next event from the queue.
   *
   * @return The next event, or null if the queue is empty.
   */
  public TdsStreamEvent peek() {
    return queue.peek();
  }

//  /**
//   * Clears the queue.
//   */
//  public void clear() {
//    queue.clear();
//    queueByteWeight.set(0);
//  }
  /**
   * Clears the queue.
   */
  public void clear() {
    queue.clear();
    queueByteWeight.set(0);
  }

  /**
   * Instantly poisons the underlying physical connection.
   */
  public void forceKillConnection() {
    transport.handleFatalConnectionError(
        new IllegalStateException("Stream cancelled mid-flight. Poisoning socket to prevent protocol desync.")
    );
  }
}
