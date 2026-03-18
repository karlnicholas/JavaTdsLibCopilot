package org.tdslib.javatdslib.reactive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class SerializedQueueDrainer<T> {
  private static final Logger logger = LoggerFactory.getLogger(SerializedQueueDrainer.class);

  protected DataSink<T> sink;
  private final Queue<T> buffer = new ConcurrentLinkedQueue<>();

  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicInteger wip = new AtomicInteger(0);
  protected final AtomicBoolean isCancelled = new AtomicBoolean(false);
  private final AtomicBoolean upstreamDone = new AtomicBoolean(false);
  private Throwable terminalError = null;

  public void setSink(DataSink<T> sink) {
    logger.trace("Downstream sink bound: {}", sink.getClass().getSimpleName());
    this.sink = sink;
  }

  public void increaseDemand(long n) {
    if (n <= 0) {
      error(new IllegalArgumentException("Request must be > 0"));
      return;
    }

    long current;
    long next;
    do {
      current = demand.get();
      next = current + n;
      if (next < 0) {
        next = Long.MAX_VALUE;
      }
    } while (!demand.compareAndSet(current, next));

    logger.trace("Downstream requested {}. Demand went from {} to {}", n, current, next);

    if (current == 0 && n > 0) {
      logger.trace("Demand was 0. Triggering onSourceStarved() to wake up network layer.");
      onSourceStarved();
    }

    onRequest(n);
    drain();
  }

  public void cancel() {
    logger.trace("Cancelled by downstream.");
    if (isCancelled.compareAndSet(false, true)) {
      buffer.clear();
      onCancel();
    }
  }

  public boolean isCancelled() {
    return isCancelled.get();
  }

  public void offer(T item) {
    if (isCancelled.get() || upstreamDone.get()) {
      logger.trace("Ignoring offer() because stream is cancelled or done.");
      return;
    }
    logger.trace("Offering item to internal buffer queue.");
    buffer.offer(item);
    drain();
  }

  public void complete() {
    logger.trace("Upstream complete signal received.");
    if (upstreamDone.compareAndSet(false, true)) {
      drain();
    }
  }

  public void error(Throwable t) {
    logger.trace("Upstream error signal received: {}", t.getMessage());
    if (upstreamDone.compareAndSet(false, true)) {
      this.terminalError = t;
      drain();
    }
  }

  protected void onRequest(long n) {}
  protected void onCancel() {}
  protected void onSourceStarved() {}
  protected void onSourceOverrun() {}

  private void drain() {
    if (sink == null) return;
    if (wip.getAndIncrement() != 0) return;

    int missed = 1;
    do {
      long requested = demand.get();
      long emitted = 0;

      logger.trace("Drain loop started. Requested: {}, Queue size: {}", requested, buffer.size());

      while (emitted != requested) {
        if (isCancelled.get()) {
          buffer.clear();
          return;
        }

        boolean done = upstreamDone.get();
        T item = buffer.poll();
        boolean empty = (item == null);

        if (done && empty) {
          if (terminalError != null) sink.pushError(terminalError);
          else sink.pushComplete();
          return;
        }

        if (empty) break;

        logger.trace("Pushing item to downstream sink.");
        sink.pushNext(item);
        emitted++;
      }

      if (emitted == requested) {
        if (isCancelled.get()) {
          buffer.clear();
          return;
        }
        boolean done = upstreamDone.get();
        if (done && buffer.isEmpty()) {
          if (terminalError != null) sink.pushError(terminalError);
          else sink.pushComplete();
          return;
        }
      }

      if (emitted != 0 && requested != Long.MAX_VALUE) {
        long remaining = demand.addAndGet(-emitted);
        logger.trace("Drain loop emitted {} items. Remaining demand: {}", emitted, remaining);

        if (remaining == 0 && !upstreamDone.get()) {
          logger.trace("Demand exhausted (0). Triggering onSourceOverrun() to halt network layer.");
          onSourceOverrun();
        }
      }

      missed = wip.addAndGet(-missed);
    } while (missed != 0);
  }
}