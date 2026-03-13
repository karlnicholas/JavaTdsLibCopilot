package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.api.TdsResult;
import org.tdslib.javatdslib.internal.TdsUpdateCount;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Consumes a flat stream of Result.Segment and sequences it into discrete Result objects.
 * A new Result is emitted for each new statement execution, typically bounded by a TdsUpdateCount.
 */
public class TdsResultBatchSequencer extends SerializedQueueDrainer<Result> implements Subscriber<Result.Segment> {
  private static final Logger logger = LoggerFactory.getLogger(TdsResultBatchSequencer.class);

  private final Publisher<Result.Segment> source;
  private Subscription upstreamSubscription;

  // Holds the currently active Result that is accepting segments
  private final AtomicReference<SegmentProcessor> currentResultProcessor = new AtomicReference<>();

  public TdsResultBatchSequencer(Publisher<Result.Segment> source) {
    this.source = source;
  }

  @Override
  protected void onRequest(long n) {
    if (upstreamSubscription == null) {
      source.subscribe(this);
    } else {
      if (currentResultProcessor.get() == null) {
        upstreamSubscription.request(1);
      }
    }
  }

  @Override
  protected void onCancel() {
    if (upstreamSubscription != null) {
      upstreamSubscription.cancel();
    }
    SegmentProcessor current = currentResultProcessor.getAndSet(null);
    if (current != null) {
      current.cancel();
    }
  }

  @Override
  public void onSubscribe(Subscription s) {
    this.upstreamSubscription = s;
    s.request(1);
  }

  @Override
  public void onNext(Result.Segment segment) {
    if (isCancelled()) return;

    SegmentProcessor processor = currentResultProcessor.get();

    // If we don't have an active Result, create one and emit it downstream
    if (processor == null) {
      processor = new SegmentProcessor();
      currentResultProcessor.set(processor);

      // Pass the processor directly to TdsResult (since it IS a SerializedQueueDrainer)
      offer(new TdsResult(processor));
    }

    processor.pushNext(segment);

    // Determine if this segment marks the end of the current Result
    if (isBoundarySegment(segment)) {
      processor.pushComplete();
      currentResultProcessor.set(null); // Next segment will trigger a new Result
    }
  }

  @Override
  public void onError(Throwable t) {
    if (isCancelled()) return;

    SegmentProcessor current = currentResultProcessor.getAndSet(null);
    if (current != null) {
      current.pushError(t);
    }
    error(t);
  }

  @Override
  public void onComplete() {
    if (isCancelled()) return;

    SegmentProcessor current = currentResultProcessor.getAndSet(null);
    if (current != null) {
      current.pushComplete();
    }
    complete();
  }

  private boolean isBoundarySegment(Result.Segment segment) {
    return segment instanceof TdsUpdateCount || segment instanceof Result.OutSegment;
  }

  /**
   * Inner processor that routes segments to the specific Result's drainer.
   * Leverages inheritance from the base drainer to handle downstream segment demand securely.
   */
  public class SegmentProcessor extends SerializedQueueDrainer<Result.Segment> {

    @Override
    protected void onRequest(long n) {
      if (upstreamSubscription != null) {
        upstreamSubscription.request(n);
      }
    }

    @Override
    protected void onCancel() {
      TdsResultBatchSequencer.this.currentResultProcessor.compareAndSet(this, null);
    }

    void pushNext(Result.Segment segment) {
      offer(segment);
    }

    void pushError(Throwable t) {
      error(t);
    }

    void pushComplete() {
      complete();
    }
  }
}