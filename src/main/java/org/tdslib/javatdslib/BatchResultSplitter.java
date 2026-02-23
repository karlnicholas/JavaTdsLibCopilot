package org.tdslib.javatdslib;

import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consumes a flat stream of Result.Segment and splits it into a stream of Result objects.
 * A new Result is emitted for each new statement execution, typically bounded by a TdsUpdateCount.
 */
public class BatchResultSplitter implements Publisher<Result>, Subscriber<Result.Segment> {
  private static final Logger logger = LoggerFactory.getLogger(BatchResultSplitter.class);

  private final Publisher<Result.Segment> source;
  private Subscriber<? super Result> downstream;
  private Subscription upstreamSubscription;

  private final AtomicBoolean isCancelled = new AtomicBoolean(false);
  private final AtomicLong demand = new AtomicLong(0);

  // Holds the currently active Result that is accepting segments
  private final AtomicReference<SegmentProcessor> currentResultProcessor = new AtomicReference<>();

  public BatchResultSplitter(Publisher<Result.Segment> source) {
    this.source = source;
  }

  @Override
  public void subscribe(Subscriber<? super Result> subscriber) {
    this.downstream = subscriber;
    subscriber.onSubscribe(new Subscription() {
      @Override
      public void request(long n) {
        if (n <= 0) {
          subscriber.onError(new IllegalArgumentException("Request must be > 0"));
          return;
        }
        addDemand(n);
        if (upstreamSubscription == null) {
          source.subscribe(BatchResultSplitter.this);
        } else {
          upstreamSubscription.request(n);
        }
      }

      @Override
      public void cancel() {
        isCancelled.set(true);
        if (upstreamSubscription != null) {
          upstreamSubscription.cancel();
        }
        SegmentProcessor current = currentResultProcessor.getAndSet(null);
        if (current != null) {
          current.cancel();
        }
      }
    });
  }

  @Override
  public void onSubscribe(Subscription s) {
    this.upstreamSubscription = s;
    // Request initially based on downstream demand
    long initialDemand = demand.get();
    if (initialDemand > 0) {
      s.request(initialDemand);
    }
  }

  @Override
  public void onNext(Result.Segment segment) {
    if (isCancelled.get()) return;

    SegmentProcessor processor = currentResultProcessor.get();

    // If we don't have an active Result, create one and emit it downstream
    if (processor == null) {
      processor = new SegmentProcessor();
      currentResultProcessor.set(processor);

      // Emit the new Result to the subscriber
      // Note: TdsResult would be your class implementing io.r2dbc.spi.Result
      downstream.onNext(new TdsResult(processor));
    }

    // Push the segment to the current Result's subscriber
    processor.onNext(segment);

    // Determine if this segment marks the end of the current Result
    if (isBoundarySegment(segment)) {
      processor.onComplete();
      currentResultProcessor.set(null); // Next segment will trigger a new Result
    }
  }

  @Override
  public void onError(Throwable t) {
    if (isCancelled.get()) return;
    SegmentProcessor current = currentResultProcessor.getAndSet(null);
    if (current != null) {
      current.onError(t);
    }
    downstream.onError(t);
  }

  @Override
  public void onComplete() {
    if (isCancelled.get()) return;
    SegmentProcessor current = currentResultProcessor.getAndSet(null);
    if (current != null) {
      current.onComplete();
    }
    downstream.onComplete();
  }

  /**
   * Determines if the segment indicates the end of a result set.
   */
  private boolean isBoundarySegment(Result.Segment segment) {
    return segment instanceof TdsUpdateCount || segment instanceof Result.OutSegment;
  }

  private void addDemand(long n) {
    long current, next;
    do {
      current = demand.get();
      next = current + n;
      if (next < 0) next = Long.MAX_VALUE;
    } while (!demand.compareAndSet(current, next));
  }

  /**
   * A simple hot publisher that routes segments to the specific Result's subscriber.
   * In a production driver, you might replace this with a UnicastProcessor if you bring in Reactor,
   * or implement a custom Trampoline queue here similar to QueryResponseTokenVisitor.
   */
  public class SegmentProcessor implements Publisher<Result.Segment> {
    private Subscriber<? super Result.Segment> resultSubscriber;
    private boolean isCompleted = false;
    private Throwable error;

    @Override
    public void subscribe(Subscriber<? super Result.Segment> subscriber) {
      this.resultSubscriber = subscriber;
      subscriber.onSubscribe(new Subscription() {
        @Override
        public void request(long n) {
          if (upstreamSubscription != null) {
            upstreamSubscription.request(n);
          }
        }

        @Override
        public void cancel() {
          BatchResultSplitter.this.currentResultProcessor.compareAndSet(SegmentProcessor.this, null);
        }
      });

      if (isCompleted) {
        if (error != null) resultSubscriber.onError(error);
        else resultSubscriber.onComplete();
      }
    }

    void onNext(Result.Segment segment) {
      if (resultSubscriber != null) {
        resultSubscriber.onNext(segment);
      }
    }

    void onError(Throwable t) {
      this.error = t;
      this.isCompleted = true;
      if (resultSubscriber != null) {
        resultSubscriber.onError(t);
      }
    }

    void onComplete() {
      this.isCompleted = true;
      if (resultSubscriber != null) {
        resultSubscriber.onComplete();
      }
    }

    void cancel() {
      this.isCompleted = true;
    }
  }
}