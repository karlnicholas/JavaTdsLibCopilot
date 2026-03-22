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
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.PartialDataColumn;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.DoneToken;
import org.tdslib.javatdslib.tokens.models.ErrorToken;
import org.tdslib.javatdslib.tokens.models.InfoToken;
import org.tdslib.javatdslib.tokens.models.ReturnStatusToken;
import org.tdslib.javatdslib.tokens.models.ReturnValueToken;
import org.tdslib.javatdslib.tokens.models.RowToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import reactor.core.scheduler.Scheduler;

import java.io.ByteArrayOutputStream;
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
  private final Scheduler workerScheduler; // Swapped from Executor

  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicInteger wip = new AtomicInteger(0);
  private final AtomicBoolean isCancelled = new AtomicBoolean(false);

  private final CountDownLatch completionLatch = new CountDownLatch(1);
  private final List<Result.Segment> receivedSegments = new CopyOnWriteArrayList<>();

  private ColMetaDataToken activeMetaData;
  private RowDrainer activeRowDrainer;
  // Add this field to buffer OUT parameters
  private final List<ReturnValueToken> activeOutParams = new java.util.ArrayList<>();

  // Replace DataSink with standard Java functional callbacks
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
        // Use Reactor's schedule method
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
          if (isCancelled.get()) return;

          int segmentsBefore = receivedSegments.size();

          TdsStreamEvent event = tokenQueue.poll();
          if (event == null) break;

          if (event instanceof ErrorEvent err) {
            pushError(err.error());
            return;
          } else if (event instanceof TokenEvent te) {
            processToken(te.token());
          } else if (event instanceof ColumnEvent ce) {
            processColumn(ce.data());
          }

          if (receivedSegments.size() > segmentsBefore) {
            emitted++;
          }
        }

        if (emitted != 0 && requested != Long.MAX_VALUE) {
          demand.addAndGet(-emitted);
        }
        missed = wip.addAndGet(-missed);
      } while (missed != 0);

    } catch (Throwable t) {
      // CRITICAL: Catch any fatal runtime exceptions to prevent infinite hangs
      // and safely propagate the error down the reactive chain.
      pushError(t);
    }
  }

  private void processToken(Token token) {
    if (token instanceof ColMetaDataToken meta) {
      this.activeMetaData = meta;
    } else if (token instanceof RowToken) {

      // Phase A: The Handoff (and Edge Case Safety)
      if (activeRowDrainer != null) {
        if (activeRowDrainer.isComplete()) {
          emitSegment(activeRowDrainer.assembleRow());
        }
      }
      // PASS THE TOKEN QUEUE HERE
      this.activeRowDrainer = new RowDrainer(activeMetaData, context, tokenQueue);

    } else if (token instanceof DoneToken done) {

      // 1. Safety flush for the very last row in the result set if it ends in a LOB
      if (activeRowDrainer != null) {
        // REMOVED: activeRowDrainer.flushPlpIfNecessary(-1);
        if (activeRowDrainer.isComplete()) {
          emitSegment(activeRowDrainer.assembleRow());
        }
        activeRowDrainer = null;
      }

      // 2. Emit Out Parameters (If any exist for this statement)
      if (!activeOutParams.isEmpty()) {
        emitSegment(new TdsOutSegment(new java.util.ArrayList<>(activeOutParams), context));
        activeOutParams.clear(); // Reset for the next statement in the batch
      }

      // 3. Emit the Update Count
      if (done.getStatus().hasCount()) {
        emitSegment(new TdsUpdateCount(done.getCount()));
      }

      // 4. Terminate the reactive stream
      if (!done.getStatus().hasMoreResults()) {
        pushComplete();
      }

    } else if (token instanceof ErrorToken error) {
      logger.debug("WE CAUGHT AN ERROR TOKEN! {}", error.getMessage());
      // Halt everything and propagate the server error immediately
      pushError(new TdsServerErrorException(
          error.getMessage(),
          error.getNumber(),
          error.getState(),
          error.getSeverity(),
          error.getServerName(),
          error.getProcName(),
          error.getLineNumber()));
    } else if (token instanceof InfoToken info) {
      logger.debug("Received InfoToken [{}]: {}", info.getNumber(), info.getMessage());

      // TDS "State" is a byte/int, so we convert it to the String sqlState R2DBC expects
      emitSegment(new TdsMessageSegment(
          (int) info.getNumber(),
          String.valueOf(info.getState()),
          info.getMessage()
      ));
    } else if (token instanceof ReturnStatusToken) {
      // INTENTIONALLY IGNORED
    } else {
      // FAIL FAST: Never let a token be ignored silently!
      logger.error("SILENT FAILURE DETECTED! Unhandled token: {}", token.getClass().getSimpleName());
    }
  }

  private void processColumn(ColumnData cd) {
    // If we receive column data but have no active drainer, drop it safely
    if (this.activeRowDrainer == null) return;

    // Delegate the data to Phase B (The Asynchronous Drain)
    this.activeRowDrainer.processColumn(cd);

    // Phase C: The Yield
    // If the drainer reports the row is finished, emit it and clean up
    if (this.activeRowDrainer.isComplete()) {
      emitSegment(this.activeRowDrainer.assembleRow());
      this.activeRowDrainer = null;
    }
  }

  private void emitSegment(Result.Segment segment) {
    logger.trace("Assembled Segment: {}", segment.getClass().getSimpleName());
    receivedSegments.add(segment);
    if (onNext != null) onNext.accept(segment);
  }

  private void pushError(Throwable error) {
    logger.debug("Stream Error: {}", error.getMessage(), error);
    completionLatch.countDown();
    if (onError != null) onError.accept(error);
  }

  private void pushComplete() {
    logger.trace("Stream Complete!");
    completionLatch.countDown();
    if (onComplete != null) onComplete.run();
  }

  public void awaitCompletion() throws InterruptedException { completionLatch.await(); }
  public List<Result.Segment> getReceivedSegments() { return receivedSegments; }
}