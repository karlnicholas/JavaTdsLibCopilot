package org.tdslib.javatdslib.api;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.reactive.DataSink;
import org.tdslib.javatdslib.reactive.SerializedQueueDrainer;
import org.tdslib.javatdslib.reactive.StatefulRow;

/**
 * Implementation of {@link Result} for the TDS protocol. This class represents the result of a
 * query execution, providing access to rows, update counts, and other result segments.
 */
public class TdsResult implements Result {
  private static final Logger logger = LoggerFactory.getLogger(TdsResult.class);
  private final Publisher<Result.Segment> source;

  /**
   * Primary constructor used by the networking layer (e.g., TdsResultBatchSequencer).
   * Bridges the mechanical SerializedQueueDrainer into a standard Reactive Streams Publisher.
   *
   * @param drainer The mechanical queue drainer managing backpressure and thread handoff.
   */
  public TdsResult(SerializedQueueDrainer<Result.Segment> drainer) {
    this.source = subscriber -> {
      // 1. Bind the mechanical pushes from the networking thread to the reactive subscriber
      drainer.setSink(new DataSink<Result.Segment>() {
        @Override public void pushNext(Result.Segment item) { subscriber.onNext(item); }
        @Override public void pushComplete() { subscriber.onComplete(); }
        @Override public void pushError(Throwable t) { subscriber.onError(t); }
      });

      // 2. Bind the reactive demand/cancellation from the worker thread back to the drainer
      subscriber.onSubscribe(new Subscription() {
        @Override public void request(long n) { drainer.increaseDemand(n); }
        @Override public void cancel() { drainer.cancel(); }
      });
    };
  }

  /**
   * Internal constructor used by operators like filter() to chain publishers
   * without needing a raw network drainer.
   */
  private TdsResult(Publisher<Result.Segment> chainedSource) {
    this.source = chainedSource;
  }

  @Override
  public Publisher<Long> getRowsUpdated() {
    return subscriber ->
        source.subscribe(
            new Subscriber<Result.Segment>() {
              private Subscription subscription;

              @Override
              public void onSubscribe(Subscription s) {
                this.subscription = s;
                subscriber.onSubscribe(
                    new Subscription() {
                      @Override
                      public void request(long n) { s.request(n); }
                      @Override
                      public void cancel() { s.cancel(); }
                    });
              }

              @Override
              public void onNext(Result.Segment segment) {
                // Actively drain any RowSegments that arrive, otherwise the network hangs forever.
                if (segment instanceof Result.RowSegment) {
                  Row row = ((Result.RowSegment) segment).row();
                  if (row instanceof StatefulRow) {
                    ((StatefulRow) row).drain();
                  }
                }

                if (segment instanceof Result.UpdateCount) {
                  subscriber.onNext(((Result.UpdateCount) segment).value());
                } else {
                  subscription.request(1);
                }
              }

              @Override
              public void onError(Throwable t) { subscriber.onError(t); }

              @Override
              public void onComplete() { subscriber.onComplete(); }
            });
  }

  @Override
  public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    if (mappingFunction == null) throw new IllegalArgumentException("mappingFunction must not be null");

    return subscriber ->
        source.subscribe(
            new Subscriber<Result.Segment>() {
              private Subscription upstream;
              private final AtomicLong demand = new AtomicLong(0);

              private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "Tds-Mapper-Worker");
                t.setDaemon(true);
                return t;
              });

              @Override
              public void onSubscribe(Subscription s) {
                this.upstream = s;
                subscriber.onSubscribe(
                    new Subscription() {
                      @Override
                      public void request(long n) {
                        if (n <= 0) return;
                        long current, next;
                        do {
                          current = demand.get();
                          if (current == Long.MAX_VALUE) break;
                          next = current + n;
                          if (next < 0) next = Long.MAX_VALUE;
                        } while (!demand.compareAndSet(current, next));

                        if (current == 0) upstream.request(1);
                      }

                      @Override
                      public void cancel() {
                        worker.shutdownNow();
                        s.cancel();
                      }
                    });
              }

              @Override
              public void onNext(Result.Segment segment) {
                if (segment instanceof Result.RowSegment) {
                  Row row = ((Result.RowSegment) segment).row();

                  try {
                    worker.execute(() -> {
                      if (worker.isShutdown()) return;
                      boolean success = false;
                      try {
                        T mapped = mappingFunction.apply(row, row.getMetadata());
                        if (mapped == null) {
                          subscriber.onError(new IllegalStateException("Mapping function returned null"));
                          upstream.cancel();
                          worker.shutdown();
                          return;
                        }
                        subscriber.onNext(mapped);
                        success = true;
                      } catch (Exception e) {
                        subscriber.onError(e);
                        upstream.cancel();
                        worker.shutdown();
                      } finally {
                        if (row instanceof StatefulRow) {
                          ((StatefulRow) row).drain();
                        }
                      }

                      if (success) {
                        long currentDemand = demand.get();
                        if (currentDemand != Long.MAX_VALUE) currentDemand = demand.decrementAndGet();
                        if (currentDemand > 0) upstream.request(1);
                      }
                    });
                  } catch (RejectedExecutionException e) {
                    // Ignored
                  }
                } else {
                  upstream.request(1);
                }
              }

              @Override
              public void onError(Throwable t) {
                try { worker.execute(() -> { if (!worker.isShutdown()) { subscriber.onError(t); worker.shutdown(); }}); } catch (Exception e) { subscriber.onError(t); }
              }

              @Override
              public void onComplete() {
                try { worker.execute(() -> { if (!worker.isShutdown()) { subscriber.onComplete(); worker.shutdown(); }}); } catch (Exception e) { subscriber.onComplete(); }
              }
            });
  }

  @Override
  public Result filter(Predicate<Result.Segment> predicate) {
    if (predicate == null) throw new IllegalArgumentException("predicate must not be null");

    return new TdsResult(
        subscriber ->
            source.subscribe(
                new Subscriber<Result.Segment>() {
                  private Subscription upstream;
                  private final AtomicLong demand = new AtomicLong(0);

                  private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "Tds-Filter-Worker");
                    t.setDaemon(true);
                    return t;
                  });

                  @Override
                  public void onSubscribe(Subscription s) {
                    this.upstream = s;
                    subscriber.onSubscribe(
                        new Subscription() {
                          @Override
                          public void request(long n) {
                            if (n <= 0) return;
                            long current, next;
                            do {
                              current = demand.get();
                              if (current == Long.MAX_VALUE) break;
                              next = current + n;
                              if (next < 0) next = Long.MAX_VALUE;
                            } while (!demand.compareAndSet(current, next));
                            if (current == 0) upstream.request(1);
                          }
                          @Override
                          public void cancel() {
                            worker.shutdownNow();
                            s.cancel();
                          }
                        });
                  }

                  @Override
                  public void onNext(Result.Segment segment) {
                    try {
                      worker.execute(() -> {
                        if (worker.isShutdown()) return;

                        boolean passed;
                        try {
                          passed = predicate.test(segment);
                        } catch (Exception e) {
                          drainIfRow(segment);
                          subscriber.onError(e);
                          upstream.cancel();
                          worker.shutdown();
                          return;
                        }

                        if (passed) {
                          subscriber.onNext(segment);

                          long currentDemand = demand.get();
                          if (currentDemand != Long.MAX_VALUE) currentDemand = demand.decrementAndGet();
                          if (currentDemand > 0) upstream.request(1);
                        } else {
                          drainIfRow(segment);
                          upstream.request(1);
                        }
                      });
                    } catch (RejectedExecutionException e) {
                      // Ignored
                    }
                  }

                  @Override
                  public void onError(Throwable t) {
                    try { worker.execute(() -> { if (!worker.isShutdown()) { subscriber.onError(t); worker.shutdown(); }}); } catch (Exception e) { subscriber.onError(t); }
                  }

                  @Override
                  public void onComplete() {
                    try { worker.execute(() -> { if (!worker.isShutdown()) { subscriber.onComplete(); worker.shutdown(); }}); } catch (Exception e) { subscriber.onComplete(); }
                  }

                  private void drainIfRow(Result.Segment segment) {
                    if (segment instanceof Result.RowSegment) {
                      Row row = ((Result.RowSegment) segment).row();
                      if (row instanceof StatefulRow) ((StatefulRow) row).drain();
                    }
                  }
                }));
  }

  @Override
  public <T> Publisher<T> flatMap(Function<Result.Segment, ? extends Publisher<? extends T>> mappingFunction) {
    if (mappingFunction == null) throw new IllegalArgumentException("mappingFunction must not be null");
    return new SegmentFlatMapPublisher<>(source, mappingFunction);
  }

  private static class SegmentFlatMapPublisher<T> implements Publisher<T> {
    private final Publisher<Result.Segment> source;
    private final Function<Result.Segment, ? extends Publisher<? extends T>> mapper;

    SegmentFlatMapPublisher(Publisher<Result.Segment> source, Function<Result.Segment, ? extends Publisher<? extends T>> mapper) {
      this.source = source;
      this.mapper = mapper;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
      source.subscribe(new FlatMapSubscriber<>(s, mapper));
    }
  }

  private static class FlatMapSubscriber<T> implements Subscriber<Result.Segment>, Subscription {
    private final Subscriber<? super T> downstream;
    private final Function<Result.Segment, ? extends Publisher<? extends T>> mapper;

    private final AtomicReference<Subscription> upstream = new AtomicReference<>();
    private final AtomicReference<Subscription> activeInner = new AtomicReference<>();
    private final AtomicLong demand = new AtomicLong();
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);
    private volatile boolean upstreamDone = false;

    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "Tds-FlatMap-Worker");
      t.setDaemon(true);
      return t;
    });

    FlatMapSubscriber(Subscriber<? super T> downstream, Function<Result.Segment, ? extends Publisher<? extends T>> mapper) {
      this.downstream = downstream;
      this.mapper = mapper;
    }

    @Override
    public void request(long n) {
      if (n <= 0) {
        downstream.onError(new IllegalArgumentException("Request must be > 0"));
        return;
      }
      addCap(demand, n);

      Subscription inner = activeInner.get();
      if (inner != null) {
        inner.request(n);
      } else {
        Subscription up = upstream.get();
        if (up != null) {
          up.request(1);
        }
      }
    }

    @Override
    public void cancel() {
      if (isCancelled.compareAndSet(false, true)) {
        worker.shutdownNow();
        Subscription inner = activeInner.getAndSet(null);
        if (inner != null) inner.cancel();
        Subscription up = upstream.getAndSet(null);
        if (up != null) up.cancel();
      }
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (upstream.compareAndSet(null, s)) {
        downstream.onSubscribe(this);
      } else {
        s.cancel();
      }
    }

    @Override
    public void onNext(Result.Segment segment) {
      if (isCancelled.get()) return;

      try {
        worker.execute(() -> {
          if (worker.isShutdown() || isCancelled.get()) return;
          try {
            Publisher<? extends T> innerPub = mapper.apply(segment);
            if (innerPub == null) {
              onError(new IllegalStateException("Mapper returned null Publisher"));
              drainIfRow(segment);
              return;
            }
            innerPub.subscribe(new InnerSubscriber(segment));
          } catch (Throwable t) {
            drainIfRow(segment);
            onError(t);
          }
        });
      } catch (RejectedExecutionException e) {
        // Ignored
      }
    }

    @Override
    public void onError(Throwable t) {
      if (isCancelled.compareAndSet(false, true)) {
        try { worker.execute(() -> { if (!worker.isShutdown()) { downstream.onError(t); worker.shutdown(); }}); } catch (Exception e) { downstream.onError(t); }
      }
    }

    @Override
    public void onComplete() {
      upstreamDone = true;
      if (activeInner.get() == null) {
        try { worker.execute(() -> { if (!worker.isShutdown()) { downstream.onComplete(); worker.shutdown(); }}); } catch (Exception e) { downstream.onComplete(); }
      }
    }

    private void drainIfRow(Result.Segment segment) {
      if (segment instanceof Result.RowSegment) {
        Row row = ((Result.RowSegment) segment).row();
        if (row instanceof StatefulRow) {
          ((StatefulRow) row).drain();
        }
      }
    }

    private class InnerSubscriber implements Subscriber<T> {
      private final Result.Segment parentSegment;

      public InnerSubscriber(Result.Segment parentSegment) {
        this.parentSegment = parentSegment;
      }

      @Override
      public void onSubscribe(Subscription s) {
        if (activeInner.compareAndSet(null, s)) {
          long d = demand.get();
          if (d > 0) s.request(d);
        } else {
          s.cancel();
        }
      }

      @Override
      public void onNext(T t) {
        if (isCancelled.get()) return;
        long currentDemand = demand.get();
        if (currentDemand != Long.MAX_VALUE) demand.decrementAndGet();
        downstream.onNext(t);
      }

      @Override
      public void onError(Throwable t) {
        drainIfRow(parentSegment);
        FlatMapSubscriber.this.onError(t);
      }

      @Override
      public void onComplete() {
        drainIfRow(parentSegment);
        activeInner.set(null);
        if (upstreamDone) {
          downstream.onComplete();
        } else {
          Subscription up = upstream.get();
          if (up != null) up.request(1);
        }
      }
    }

    private void addCap(AtomicLong requested, long n) {
      long current, next;
      do {
        current = requested.get();
        if (current == Long.MAX_VALUE) return;
        next = current + n;
        if (next < 0) next = Long.MAX_VALUE;
      } while (!requested.compareAndSet(current, next));
    }
  }
}