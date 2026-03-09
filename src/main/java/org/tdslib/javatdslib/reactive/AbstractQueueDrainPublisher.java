package org.tdslib.javatdslib.reactive;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractQueueDrainPublisher<T> implements Publisher<T>, Subscription {
  private static final Logger logger = LoggerFactory.getLogger(AbstractQueueDrainPublisher.class);

  protected Subscriber<? super T> subscriber;
  private final Queue<T> buffer = new ConcurrentLinkedQueue<>();

  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicInteger wip = new AtomicInteger(0);
  protected final AtomicBoolean isCancelled = new AtomicBoolean(false);
  private final AtomicBoolean upstreamDone = new AtomicBoolean(false);
  private Throwable terminalError = null;

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    logger.trace("[AbstractQueueDrain] Downstream subscribed: {}", subscriber.getClass().getSimpleName());
    this.subscriber = subscriber;
    subscriber.onSubscribe(this);
  }

  @Override
  public void request(long n) {
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

    logger.trace("[AbstractQueueDrain] Downstream requested {}. Demand went from {} to {}", n, current, next);

    if (current == 0 && n > 0) {
      logger.trace("[AbstractQueueDrain] Demand was 0. Triggering onResume() to wake up network layer.");
      onResume();
    }

    onRequest(n);
    drain();
  }

  @Override
  public void cancel() {
    logger.trace("[AbstractQueueDrain] Cancelled by downstream.");
    if (isCancelled.compareAndSet(false, true)) {
      buffer.clear();
      onCancel();
    }
  }

  protected void emit(T item) {
    if (isCancelled.get() || upstreamDone.get()) {
      logger.trace("[AbstractQueueDrain] Ignoring emit() because stream is cancelled or done.");
      return;
    }
    logger.trace("[AbstractQueueDrain] Emitting item to internal buffer queue.");
    buffer.offer(item);
    drain();
  }

  protected void complete() {
    logger.trace("[AbstractQueueDrain] Upstream complete signal received.");
    if (upstreamDone.compareAndSet(false, true)) {
      drain();
    }
  }

  protected void error(Throwable t) {
    logger.trace("[AbstractQueueDrain] Upstream error signal received: {}", t.getMessage());
    if (upstreamDone.compareAndSet(false, true)) {
      this.terminalError = t;
      drain();
    }
  }

  protected void onRequest(long n) {}
  protected void onCancel() {}
  protected void onResume() {}
  protected void onSuspend() {}

  private void drain() {
    if (subscriber == null) return;
    if (wip.getAndIncrement() != 0) return;

    int missed = 1;
    do {
      long requested = demand.get();
      long emitted = 0;

      logger.trace("[AbstractQueueDrain] Drain loop started. Requested: {}, Queue size: {}", requested, buffer.size());

      while (emitted != requested) {
        if (isCancelled.get()) {
          buffer.clear();
          return;
        }

        boolean done = upstreamDone.get();
        T item = buffer.poll();
        boolean empty = (item == null);

        if (done && empty) {
          if (terminalError != null) subscriber.onError(terminalError);
          else subscriber.onComplete();
          return;
        }

        if (empty) break;

        logger.trace("[AbstractQueueDrain] Pushing item to downstream subscriber.onNext().");
        subscriber.onNext(item);
        emitted++;
      }

      if (emitted == requested) {
        if (isCancelled.get()) {
          buffer.clear();
          return;
        }
        boolean done = upstreamDone.get();
        if (done && buffer.isEmpty()) {
          if (terminalError != null) subscriber.onError(terminalError);
          else subscriber.onComplete();
          return;
        }
      }

      if (emitted != 0 && requested != Long.MAX_VALUE) {
        long remaining = demand.addAndGet(-emitted);
        logger.trace("[AbstractQueueDrain] Drain loop emitted {} items. Remaining demand: {}", emitted, remaining);

        if (remaining == 0 && !upstreamDone.get()) {
          logger.trace("[AbstractQueueDrain] Demand exhausted (0). Triggering onSuspend() to halt network layer.");
          onSuspend();
        }
      }

      missed = wip.addAndGet(-missed);
    } while (missed != 0);
  }
}