package org.tdslib.javatdslib.protocol;

import io.r2dbc.spi.Result;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.api.TdsResult;
import org.tdslib.javatdslib.reactive.AbstractQueueDrainPublisher;

/**
 * Consumes a flat stream of Result.Segment and splits it into a stream of Result objects.
 * A new Result is emitted for each new statement execution, typically bounded by a TdsUpdateCount.
 */
public class BatchResultSplitter extends AbstractQueueDrainPublisher<Result>
    implements Subscriber<Result.Segment> {
  private static final Logger logger = LoggerFactory.getLogger(BatchResultSplitter.class);

  private final Publisher<Result.Segment> source;
  private Subscription upstreamSubscription;

  // Holds the currently active Result that is accepting segments
  private final AtomicReference<SegmentProcessor> currentResultProcessor = new AtomicReference<>();

  /**
   * Creates a new BatchResultSplitter.
   *
   * @param source the upstream publisher of Result.Segment
   */
  public BatchResultSplitter(Publisher<Result.Segment> source) {
    this.source = source;
  }

  @Override
  protected void onRequest(long n) {
    // Triggered when the downstream subscriber requests the next Result object.
    if (upstreamSubscription == null) {
      source.subscribe(this);
    } else {
      // If we are between results, request 1 segment from upstream
      // to trigger the creation of the next Result.
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
      current.cancel(); // Cancel the inner publisher
    }
  }

  @Override
  public void onSubscribe(Subscription s) {
    this.upstreamSubscription = s;
    // Kickstart the flow by requesting the very first segment, which will create the first Result
    s.request(1);
  }

  @Override
  public void onNext(Result.Segment segment) {
    if (isCancelled.get()) {
      return;
    }

    SegmentProcessor processor = currentResultProcessor.get();

    // If we don't have an active Result, create one and emit it downstream
    if (processor == null) {
      processor = new SegmentProcessor();
      currentResultProcessor.set(processor);

      // Emit the new Result to the outer Result subscriber using the base class method
      emit(new TdsResult(processor));
    }

    // Push the segment to the inner Result's subscriber
    processor.pushNext(segment);

    // Determine if this segment marks the end of the current Result
    if (isBoundarySegment(segment)) {
      processor.pushComplete();
      currentResultProcessor.set(null); // Next segment will trigger a new Result
    }
  }

  @Override
  public void onError(Throwable t) {
    if (isCancelled.get()) {
      return;
    }
    SegmentProcessor current = currentResultProcessor.getAndSet(null);
    if (current != null) {
      current.pushError(t);
    }
    error(t); // Fail outer publisher
  }

  @Override
  public void onComplete() {
    if (isCancelled.get()) {
      return;
    }
    SegmentProcessor current = currentResultProcessor.getAndSet(null);
    if (current != null) {
      current.pushComplete();
    }
    complete(); // Complete outer publisher
  }

  /**
   * Determines if the segment indicates the end of a result set.
   */
  private boolean isBoundarySegment(Result.Segment segment) {
    return segment instanceof TdsUpdateCount || segment instanceof Result.OutSegment;
  }

  /**
   * Inner publisher that routes segments to the specific Result's subscriber.
   * Extends the base class to handle downstream segment demand securely.
   */
  public class SegmentProcessor extends AbstractQueueDrainPublisher<Result.Segment> {

    @Override
    protected void onRequest(long n) {
      // When the downstream subscriber for *this specific Result* requests segments,
      // we pass that demand upstream to the original network stream.
      if (upstreamSubscription != null) {
        upstreamSubscription.request(n);
      }
    }

    @Override
    protected void onCancel() {
      // If the downstream subscriber cancels this specific Result,
      // we detach it, but we do not cancel the whole network stream.
      BatchResultSplitter.this.currentResultProcessor.compareAndSet(this, null);
    }

    void pushNext(Result.Segment segment) {
      emit(segment);
    }

    void pushError(Throwable t) {
      error(t);
    }

    void pushComplete() {
      complete();
    }
  }
}