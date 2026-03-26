package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.internal.TdsUpdateCount;
import org.tdslib.javatdslib.protocol.TdsServerErrorException;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
import org.tdslib.javatdslib.reactive.events.ErrorEvent;
import org.tdslib.javatdslib.reactive.events.TdsStreamEvent;
import org.tdslib.javatdslib.reactive.events.TokenEvent;
import org.tdslib.javatdslib.tokens.ColumnData;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.DoneToken;
import org.tdslib.javatdslib.tokens.models.ErrorToken;
import org.tdslib.javatdslib.tokens.models.InfoToken;
import org.tdslib.javatdslib.tokens.models.OrderToken;
import org.tdslib.javatdslib.tokens.models.ReturnStatusToken;
import org.tdslib.javatdslib.tokens.models.ReturnValueToken;
import org.tdslib.javatdslib.tokens.models.RowToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class AsyncWorkerSink {
  private static final Logger logger = LoggerFactory.getLogger(AsyncWorkerSink.class);

  private final TdsTokenQueue tokenQueue;
  private final ConnectionContext context;
  private final Scheduler workerScheduler;

  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicInteger wip = new AtomicInteger(0);
  private final AtomicBoolean isCancelled = new AtomicBoolean(false);
  private final AtomicBoolean isPaused = new AtomicBoolean(false); // Stream Lock

  private final CountDownLatch completionLatch = new CountDownLatch(1);
  private final List<Result.Segment> receivedSegments = new CopyOnWriteArrayList<>();

  private ColMetaDataToken activeMetaData;
  private RowDrainer activeRowDrainer;
  private final List<ReturnValueToken> activeOutParams = new java.util.ArrayList<>();

  private Consumer<Result.Segment> onNext;
  private Consumer<Throwable> onError;
  private Runnable onComplete;

  public AsyncWorkerSink(TdsTokenQueue tokenQueue, ConnectionContext context, Scheduler workerScheduler) {
    this.tokenQueue = tokenQueue;
    this.context = context;
    this.workerScheduler = workerScheduler;
    this.tokenQueue.setOnEventAvailableCallback(this::scheduleDrain);
  }

  public void setCallbacks(Consumer<Result.Segment> onNext, Consumer<Throwable> onError, Runnable onComplete) {
    this.onNext = onNext;
    this.onError = onError;
    this.onComplete = onComplete;
  }

  public void request(long n) {
    if (n > 0) {
      demand.addAndGet(n);
      scheduleDrain();
    }
  }

  public void cancel() {
    isCancelled.set(true);
    tokenQueue.clear();
  }

  private void scheduleDrain() {
    if (wip.getAndIncrement() == 0) {
      if (workerScheduler != null) {
        workerScheduler.schedule(this::drain);
      } else {
        drain();
      }
    }
  }

  private void drain() {
    int missed = 1;
    try {
      do {
        long requested = demand.get();
        long emitted = 0;

        while (emitted != requested) {
          // CRITICAL FIX: Use 'break' to gracefully exit and decrement WIP
          if (isCancelled.get() || isPaused.get()) {
            break;
          }

          int segmentsBefore = receivedSegments.size();

          TdsStreamEvent event = tokenQueue.poll();
          if (event == null) break;

          if (event instanceof ErrorEvent err) {
            pushError(err.error());
            isCancelled.set(true);
            break; // Break instead of return to close out WIP safely
          } else if (event instanceof TokenEvent te) {
            processToken(te.token());
          } else if (event instanceof ColumnEvent ce) {
            processColumn(ce.data());
          }

          if (receivedSegments.size() > segmentsBefore) {
            emitted++;
          }

          // Check immediately after processing just in case a LOB paused us
          if (isPaused.get()) {
            break;
          }
        }

        if (emitted != 0 && requested != Long.MAX_VALUE) {
          demand.addAndGet(-emitted);
        }
        missed = wip.addAndGet(-missed);
      } while (missed != 0);

    } catch (Throwable t) {
      pushError(t);
    }
  }

  private void processToken(Token token) {
    if (token instanceof ColMetaDataToken meta) {
      this.activeMetaData = meta;
    } else if (token instanceof RowToken) {

      if (activeRowDrainer != null && activeRowDrainer.isComplete()) {
        emitSegment(activeRowDrainer.assembleRow());
      }
      this.activeRowDrainer = new RowDrainer(activeMetaData, context, tokenQueue);

    } else if (token instanceof DoneToken done) {

      if (activeRowDrainer != null) {
        if (activeRowDrainer.isComplete()) {
          emitSegment(activeRowDrainer.assembleRow());
        }
        activeRowDrainer = null;
      }

      if (!activeOutParams.isEmpty()) {
        emitSegment(new TdsOutSegment(new java.util.ArrayList<>(activeOutParams), context));
        activeOutParams.clear();
      }

      if (done.getStatus().hasCount()) {
        emitSegment(new TdsUpdateCount(done.getCount()));
      }

      if (!done.getStatus().hasMoreResults()) {
        pushComplete();
      }

    } else if (token instanceof ErrorToken error) {
      pushError(new TdsServerErrorException(
          error.getMessage(), error.getNumber(), error.getState(),
          error.getSeverity(), error.getServerName(), error.getProcName(), error.getLineNumber()));
    } else if (token instanceof InfoToken info) {
      emitSegment(new TdsMessageSegment((int) info.getNumber(), String.valueOf(info.getState()), info.getMessage()));
    } else if (token instanceof ReturnStatusToken || token instanceof OrderToken) {
      // Ignored
    }
  }

  private void processColumn(ColumnData cd) {
    if (this.activeRowDrainer == null) return;

    this.activeRowDrainer.processColumn(cd);

    if (this.activeRowDrainer.isComplete()) {
      StatefulRow row = this.activeRowDrainer.assembleRow();

      // Hand the pause/resume power directly to the Row
      row.setAsyncCallbacks(
          () -> isPaused.set(true),
          () -> {
            isPaused.set(false);
            scheduleDrain(); // Wake up when Clob is done
          }
      );

      emitSegment(row); // User's map() executes here
      this.activeRowDrainer = null;
    }
  }

  private void emitSegment(Result.Segment segment) {
    receivedSegments.add(segment);
    try {
      if (onNext != null) onNext.accept(segment);
    } catch (Throwable t) {
      pushError(t);
      throw t;
    }
  }

  private void pushError(Throwable error) {
    logger.debug("Stream Error: {}", error.getMessage());
    completionLatch.countDown();
    if (onError != null) onError.accept(error);
  }

  private void pushComplete() {
    completionLatch.countDown();
    if (onComplete != null) onComplete.run();
  }

  public void awaitCompletion() throws InterruptedException { completionLatch.await(); }
  public List<Result.Segment> getReceivedSegments() { return receivedSegments; }
}