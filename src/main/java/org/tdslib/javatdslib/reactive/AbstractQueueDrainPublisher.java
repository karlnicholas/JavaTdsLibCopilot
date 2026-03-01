package org.tdslib.javatdslib.reactive;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A reusable Reactive Streams Publisher that manages backpressure
 * using an unbounded concurrent queue and a standard drain loop.
 */
public abstract class AbstractQueueDrainPublisher<T> implements Publisher<T>, Subscription {

  protected Subscriber<? super T> subscriber;
  private final Queue<T> buffer = new ConcurrentLinkedQueue<>();

  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicInteger wip = new AtomicInteger(0);
  protected final AtomicBoolean isCancelled = new AtomicBoolean(false);
  private final AtomicBoolean upstreamDone = new AtomicBoolean(false);
  private Throwable terminalError = null;

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    this.subscriber = subscriber;
    subscriber.onSubscribe(this);
  }

  @Override
  public void request(long n) {
    if (n <= 0) {
      error(new IllegalArgumentException("Request must be > 0"));
      return;
    }

    // Add demand safely avoiding overflow
    long current, next;
    do {
      current = demand.get();
      next = current + n;
      if (next < 0) next = Long.MAX_VALUE;
    } while (!demand.compareAndSet(current, next));

    // Optional hook for subclasses to trigger work when demand arrives
    onRequest(n);

    drain();
  }

  @Override
  public void cancel() {
    if (isCancelled.compareAndSet(false, true)) {
      buffer.clear();
      onCancel(); // Hook for subclasses to cancel upstream work
    }
  }

  /**
   * Called to push a new item into the queue and trigger a drain.
   */
  protected void emit(T item) {
    if (isCancelled.get() || upstreamDone.get()) return;
    buffer.offer(item);
    drain();
  }

  /**
   * Called to signal that no more items will be emitted.
   */
  protected void complete() {
    if (upstreamDone.compareAndSet(false, true)) {
      drain();
    }
  }

  /**
   * Called to signal a terminal error.
   */
  protected void error(Throwable t) {
    if (upstreamDone.compareAndSet(false, true)) {
      this.terminalError = t;
      drain();
    }
  }

  /**
   * Subclasses can override this to initiate network requests
   * on the first downstream demand.
   */
  protected void onRequest(long n) { }

  /**
   * Subclasses can override this to clean up network resources
   * when the downstream cancels.
   */
  protected void onCancel() { }

  private void drain() {
    if (subscriber == null) return;
    if (wip.getAndIncrement() != 0) return;

    int missed = 1;
    do {
      long requested = demand.get();
      long emitted = 0;

      while (emitted != requested) {
        if (isCancelled.get()) {
          buffer.clear();
          return;
        }

        boolean done = upstreamDone.get();
        T item = buffer.poll();
        boolean empty = (item == null);

        // Check for terminal states
        if (done && empty) {
          if (terminalError != null) {
            subscriber.onError(terminalError);
          } else {
            subscriber.onComplete();
          }
          return;
        }

        if (empty) break;

        subscriber.onNext(item);
        emitted++;
      }

      // Check terminal states when demand is fully met
      if (emitted == requested) {
        if (isCancelled.get()) {
          buffer.clear();
          return;
        }
        boolean done = upstreamDone.get();
        if (done && buffer.isEmpty()) {
          if (terminalError != null) {
            subscriber.onError(terminalError);
          } else {
            subscriber.onComplete();
          }
          return;
        }
      }

      if (emitted != 0 && requested != Long.MAX_VALUE) {
        demand.addAndGet(-emitted);
      }

      missed = wip.addAndGet(-missed);
    } while (missed != 0);
  }
}