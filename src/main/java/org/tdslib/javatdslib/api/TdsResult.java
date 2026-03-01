package org.tdslib.javatdslib.api;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class TdsResult implements Result {

  private final Publisher<Result.Segment> source;

  public TdsResult(Publisher<Result.Segment> source) {
    this.source = source;
  }

  @Override
  public Publisher<Long> getRowsUpdated() {
    return subscriber -> source.subscribe(new Subscriber<Result.Segment>() {
      private Subscription subscription;

      @Override
      public void onSubscribe(Subscription s) {
        this.subscription = s;
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) { s.request(n); }

          @Override
          public void cancel() { s.cancel(); }
        });
      }

      @Override
      public void onNext(Result.Segment segment) {
        if (segment instanceof Result.UpdateCount) {
          subscriber.onNext(((Result.UpdateCount) segment).value());
        } else {
          // Skip non-update counts and ask for the next segment
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

    return subscriber -> source.subscribe(new Subscriber<Result.Segment>() {
      private Subscription subscription;

      @Override
      public void onSubscribe(Subscription s) {
        this.subscription = s;
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) { s.request(n); }

          @Override
          public void cancel() { s.cancel(); }
        });
      }

      @Override
      public void onNext(Result.Segment segment) {
        if (segment instanceof Result.RowSegment) {
          Row row = ((Result.RowSegment) segment).row();
          try {
            T mapped = mappingFunction.apply(row, row.getMetadata());
            if (mapped == null) {
              subscriber.onError(new IllegalStateException("Mapping function returned null"));
              subscription.cancel();
              return;
            }
            subscriber.onNext(mapped);
          } catch (Exception e) {
            subscriber.onError(e);
            subscription.cancel();
          }
        } else {
          // Not a row, skip and request the next segment
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
  public Result filter(Predicate<Result.Segment> predicate) {
    if (predicate == null) throw new IllegalArgumentException("predicate must not be null");

    return new TdsResult(subscriber -> source.subscribe(new Subscriber<Result.Segment>() {
      private Subscription subscription;

      @Override
      public void onSubscribe(Subscription s) {
        this.subscription = s;
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) { s.request(n); }

          @Override
          public void cancel() { s.cancel(); }
        });
      }

      @Override
      public void onNext(Result.Segment segment) {
        if (predicate.test(segment)) {
          subscriber.onNext(segment);
        } else {
          subscription.request(1);
        }
      }

      @Override
      public void onError(Throwable t) { subscriber.onError(t); }

      @Override
      public void onComplete() { subscriber.onComplete(); }
    }));
  }

  @Override
  public <T> Publisher<T> flatMap(Function<Result.Segment, ? extends Publisher<? extends T>> mappingFunction) {
    if (mappingFunction == null) throw new IllegalArgumentException("mappingFunction must not be null");
    return new SegmentFlatMapPublisher<>(source, mappingFunction);
  }

  // --- Internal ConcatMap Publisher ---

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
        Publisher<? extends T> innerPub = mapper.apply(segment);
        if (innerPub == null) {
          onError(new IllegalStateException("Mapper returned null Publisher"));
          return;
        }
        innerPub.subscribe(new InnerSubscriber());
      } catch (Throwable t) {
        onError(t);
      }
    }

    @Override
    public void onError(Throwable t) {
      if (isCancelled.compareAndSet(false, true)) {
        downstream.onError(t);
      }
    }

    @Override
    public void onComplete() {
      upstreamDone = true;
      if (activeInner.get() == null) {
        downstream.onComplete();
      }
    }

    private class InnerSubscriber implements Subscriber<T> {
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

        // Only decrement if demand isn't unbounded
        long currentDemand = demand.get();
        if (currentDemand != Long.MAX_VALUE) {
          demand.decrementAndGet();
        }
        downstream.onNext(t);
      }

      @Override
      public void onError(Throwable t) {
        FlatMapSubscriber.this.onError(t);
      }

      @Override
      public void onComplete() {
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