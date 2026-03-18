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
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A reactive publisher that bridges the synchronous Netty network thread
 * and the asynchronous R2DBC worker thread. It throttles network reads based on memory usage.
 */
public class TdsEventPublisher implements TdsDecoderSink {
  private static final Logger logger = LoggerFactory.getLogger(TdsEventPublisher.class);

  // --- Network Throttling Configuration ---
  private static final int HIGH_WATERMARK = 5 * 1024 * 1024; // 5 MB
  private static final int LOW_WATERMARK = 1 * 1024 * 1024;  // 1 MB

  private final TdsTransport transport;
  private final ConnectionContext context;
  private final Executor workerExecutor;

  // Notice downstream is now expecting TdsStreamEvents, not Result.Segments
  private DataSink<TdsStreamEvent> downstreamSink;

  private final Queue<TdsStreamEvent> networkQueue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger queueByteWeight = new AtomicInteger(0);
  private final AtomicBoolean isNetworkSuspended = new AtomicBoolean(false);

  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicInteger wip = new AtomicInteger(0);
  private final AtomicBoolean isCancelled = new AtomicBoolean(false);

  public TdsEventPublisher(TdsTransport transport, ConnectionContext context, Executor workerExecutor) {
    this.transport = transport;
    this.context = context;
    this.workerExecutor = workerExecutor;
  }

  public void setDownstreamSink(DataSink<TdsStreamEvent> downstreamSink) {
    this.downstreamSink = downstreamSink;
  }

  // ====================================================================================
  // NETWORK THREAD: SINK IMPLEMENTATION
  // ====================================================================================

  @Override
  public void onToken(Token token) { offerEvent(new TokenEvent(token)); }

  @Override
  public void onColumnData(ColumnData data) { offerEvent(new ColumnEvent(data)); }

  @Override
  public void onError(Throwable error) { offerEvent(new ErrorEvent(error)); }

  private void offerEvent(TdsStreamEvent event) {
    networkQueue.offer(event);
    int currentWeight = queueByteWeight.addAndGet(event.getByteWeight());

    if (currentWeight > HIGH_WATERMARK && isNetworkSuspended.compareAndSet(false, true)) {
      logger.debug("High Watermark reached ({} bytes). Suspending network.", currentWeight);
      transport.suspendNetworkRead();
    }

    scheduleDrain();
  }

  // ====================================================================================
  // WORKER THREAD: DEMAND & DRAIN
  // ====================================================================================

  public void request(long n) {
    if (n > 0) {
      demand.addAndGet(n);
      scheduleDrain();
    }
  }

  public void cancel() {
    isCancelled.set(true);
    transport.cancelCurrent();
  }

  private void scheduleDrain() {
    if (wip.getAndIncrement() == 0) {
      if (workerExecutor != null) {
        workerExecutor.execute(this::drain);
      } else {
        drain();
      }
    }
  }

  private void drain() {
    int missed = 1;
    do {
      long requested = demand.get();
      long emitted = 0;

      while (emitted != requested) {
        if (isCancelled.get()) {
          networkQueue.clear();
          return;
        }

        TdsStreamEvent event = networkQueue.poll();
        if (event == null) break;

        // Manage Low Watermark
        int weight = queueByteWeight.addAndGet(-event.getByteWeight());
        if (weight < LOW_WATERMARK && isNetworkSuspended.compareAndSet(true, false)) {
          logger.debug("Low Watermark reached ({} bytes). Resuming network.", weight);
          transport.resumeNetworkRead();
        }

        // Push raw event downstream
        if (event instanceof ErrorEvent err) {
          downstreamSink.pushError(err.error());
          return;
        } else {
          downstreamSink.pushNext(event);
          emitted++;
        }
      }

      if (emitted != 0 && requested != Long.MAX_VALUE) {
        demand.addAndGet(-emitted);
      }

      missed = wip.addAndGet(-missed);
    } while (missed != 0);
  }
}