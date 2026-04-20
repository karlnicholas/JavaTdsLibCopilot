package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.impl.TdsMessageSegment;
import org.tdslib.javatdslib.impl.TdsOutSegment;
import org.tdslib.javatdslib.impl.TdsRow;
import org.tdslib.javatdslib.impl.TdsUpdateCount;
import org.tdslib.javatdslib.protocol.EnvChangeApplier;
import org.tdslib.javatdslib.protocol.TdsServerErrorException;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
import org.tdslib.javatdslib.reactive.events.ErrorEvent;
import org.tdslib.javatdslib.reactive.events.TdsStreamEvent;
import org.tdslib.javatdslib.reactive.events.TokenEvent;
import org.tdslib.javatdslib.tokens.ColumnData;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.DoneToken;
import org.tdslib.javatdslib.tokens.models.EnvChangeToken;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Handles processing of TDS tokens from the token queue, emitting results downstream.
 */
public class AsyncWorkerSink {
  private static final Logger logger = LoggerFactory.getLogger(AsyncWorkerSink.class);

  private final TdsTokenQueue tokenQueue;
  private final ConnectionContext context;
  private final Scheduler workerScheduler;

  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicInteger wip = new AtomicInteger(0);
  private final AtomicBoolean isCancelled = new AtomicBoolean(false);
  private final AtomicBoolean isPaused = new AtomicBoolean(false); // Stream Lock

  private boolean segmentEmitted = false;
  private volatile boolean isWireClean = false;
  private volatile boolean isDiscarding = false;
  private volatile boolean isAttentionPending = false;

  private ColMetaDataToken activeMetaData;
  private RowDrainer activeRowDrainer;
  private final List<ReturnValueToken> activeOutParams = new java.util.ArrayList<>();

  private Throwable pendingError = null;

  private Consumer<Result.Segment> onNext;
  private Consumer<Throwable> onError;
  private Runnable onComplete;

  /**
   * Constructs a new AsyncWorkerSink.
   *
   * @param tokenQueue      The token queue from which to read events.
   * @param context         The connection context.
   * @param workerScheduler The scheduler for asynchronous work.
   */
  public AsyncWorkerSink(
      TdsTokenQueue tokenQueue, ConnectionContext context, Scheduler workerScheduler) {
    this.tokenQueue = tokenQueue;
    this.context = context;
    this.workerScheduler = workerScheduler;
    this.tokenQueue.setOnEventAvailableCallback(this::scheduleDrain);
  }

  /**
   * Sets the callbacks for successful next events, errors, and completion.
   *
   * @param onNext     Consumer called with each new Segment.
   * @param onError    Consumer called if an error occurs.
   * @param onComplete Runnable called when processing finishes.
   */
  public void setCallbacks(
      Consumer<Result.Segment> onNext, Consumer<Throwable> onError, Runnable onComplete) {
    this.onNext = onNext;
    this.onError = onError;
    this.onComplete = onComplete;
  }

  /**
   * Requests processing of {@code n} additional events.
   *
   * @param n The amount of events requested.
   */
  public void request(long n) {
    if (n > 0) {
      demand.addAndGet(n);
      scheduleDrain();
    }
  }

//  /**
//   * Cancels processing. If the wire is not clean, enters Discard Mode.
//   */
//  public void cancel() {
//    if (isCancelled.compareAndSet(false, true)) {
//      if (!isWireClean) {
//        logger.debug("Stream cancelled mid-flight. Entering Graceful Discard Mode.");
//        this.isDiscarding = true;
//        scheduleDrain(); // Wake up the drain loop to vacuum the remaining bytes
//      } else {
//        tokenQueue.clear();
//      }
//    }
//  }

  public void cancel() {
    cancel(false);
  }

  /**
   * Cancels processing. If the wire is not clean, enters Discard Mode.
   * @param expectAttentionAck If true, registers a cryptographic debt requiring a DONE_ATTN token.
   */
  public void cancel(boolean expectAttentionAck) {
    if (isCancelled.compareAndSet(false, true)) {
      if (!isWireClean) {
        logger.debug("Stream cancelled mid-flight. Entering Graceful Discard Mode.");
        this.isDiscarding = true;

        if (expectAttentionAck) {
          this.isAttentionPending = true;
          logger.debug("Attention debt registered. Lock will hold until DONE_ATTN (0x0020) is received.");
        }

        scheduleDrain(); // Wake up the drain loop to vacuum the remaining bytes
      } else {
        tokenQueue.clear();
      }
    }
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

    // The do-while loop now sits OUTSIDE the try-catch.
    // This guarantees the wip lock is always decremented before the thread exits.
    do {
      long requested = demand.get();
      long emitted = 0;

//      try {
//        while (emitted != requested) {
//          if (isCancelled.get() || isPaused.get()) {
//            break;
//          }
//
//          // Reset the tracker for this specific loop iteration
//          this.segmentEmitted = false;
      try {
        // Keep spinning if we need to emit OR if we are actively vacuuming the wire
        while (emitted != requested || isDiscarding) {
          // Only break if we are paused, OR if we are cancelled but NOT discarding
          if ((isCancelled.get() && !isDiscarding) || isPaused.get()) {
            break;
          }

          // Reset the tracker for this specific loop iteration
          this.segmentEmitted = false;
          TdsStreamEvent event = tokenQueue.poll();
          if (event == null) {
            break;
          }

          if (event instanceof ErrorEvent err) {
            pushError(err.error());
            isCancelled.set(true);
            break;
          } else if (event instanceof TokenEvent te) {
            processToken(te.token());
          } else if (event instanceof ColumnEvent ce) {
            processColumn(ce.data());
          }

          if (this.segmentEmitted) {
            emitted++;
          }

          // Check immediately after processing just in case a LOB paused us
          if (isPaused.get()) {
            break;
          }
        }
      } catch (Throwable t) {
        // If downstream throws an error or the parser crashes, we catch it here.
        // We push the error and cancel the stream to stop further processing.
        pushError(t);
        isCancelled.set(true);
      }

      // Because the try-catch is closed, execution ALWAYS reaches these lines
      if (emitted != 0 && requested != Long.MAX_VALUE) {
        demand.addAndGet(-emitted);
      }

      // The lock is safely decremented, preventing the silent deadlock.
      missed = wip.addAndGet(-missed);

    } while (missed != 0);
  }

    private void processToken(Token token) {
// THE VACUUM: If discarding, drop everything until the appropriate DONE token
      if (this.isDiscarding) {
        if (token instanceof DoneToken done) {
          boolean isClean = false;

          if (this.isAttentionPending) {
            // Cryptographic Debt: MUST wait for the Attention Acknowledgment bit (0x0020)
            // NOTE: If your DoneToken.Status class doesn't have isAttention(),
            // implement it by checking if (statusValue & 0x0020) != 0
            if (done.getStatus().isAttention()) {
              logger.debug("Attention Acknowledged (DONE_ATTN received). Debt paid.");
              isClean = true;
            } else {
              logger.trace("Ignoring natural DONE token. Attention debt still pending.");
            }
          } else if (!done.getStatus().hasMoreResults()) {
            // Passive Drain: Wait for the natural end of the batch
            isClean = true;
          }

          if (isClean) {
            logger.debug("Graceful Discard complete. Wire is clean.");
            this.isDiscarding = false;
            this.isAttentionPending = false;
            this.isWireClean = true;
            this.activeRowDrainer = null;
            this.tokenQueue.clear();
            pushComplete(); // Safely triggers TdsTransport to release the lock
          }
        }
        return;
      }

      if (token instanceof ColMetaDataToken meta) {
        this.activeMetaData = meta;
      } else if (token instanceof RowToken) {
          // FIXED: Use the new two-phase lifecycle flags
      if (activeRowDrainer != null && activeRowDrainer.isReadyToYield()
          && !activeRowDrainer.isRowEmitted()) {
        emitSegment(activeRowDrainer.assembleRow());
      }
      this.activeRowDrainer = new RowDrainer(activeMetaData, context, tokenQueue);

//    } else if (token instanceof DoneToken done) {
//
//      if (activeRowDrainer != null) {
      } else if (token instanceof DoneToken done) {

        // MARK WIRE CLEAN
        boolean noMoreResults = !done.getStatus().hasMoreResults();
        if (noMoreResults && this.pendingError == null) {
          this.isWireClean = true;
        }

      if (activeRowDrainer != null) {
        // FIXED: Use the new two-phase lifecycle flags
        if (activeRowDrainer.isReadyToYield() && !activeRowDrainer.isRowEmitted()) {
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

      // --- NEW: Delayed Error Emission ---
      if (this.pendingError != null) {
        pushError(this.pendingError);
        this.pendingError = null; // Clear it to be safe
      } else if (!done.getStatus().hasMoreResults()) {
        pushComplete();
      }
    } else if (token instanceof ErrorToken error) {
      this.pendingError = new TdsServerErrorException(
          error.getMessage(), error.getNumber(), error.getState(),
          error.getSeverity(), error.getServerName(), error.getProcName(), error.getLineNumber());
    } else if (token instanceof InfoToken info) {
      emitSegment(new TdsMessageSegment(
          (int) info.getNumber(), String.valueOf(info.getState()), info.getMessage()));

      // --> ADD THIS BLOCK <--
    } else if (token instanceof ReturnValueToken retVal) {
      activeOutParams.add(retVal);

    } else if (token instanceof ReturnStatusToken || token instanceof OrderToken) {
      // Ignored
    } else if (token instanceof EnvChangeToken envChangeToken) {
      EnvChangeApplier.apply(envChangeToken, context);
    }
  }

//  private void processColumn(ColumnData cd) {
//    if (this.activeRowDrainer == null) {
//      return;
//    }
//
//    this.activeRowDrainer.processColumn(cd);

  private void processColumn(ColumnData cd) {
    // Drop LOB chunks immediately if vacuuming
    if (this.isDiscarding || this.activeRowDrainer == null) {
      return;
    }

    this.activeRowDrainer.processColumn(cd);
    // ... (keep rest exactly as is)
    //
    // PHASE 1: Emit if the row is logically ready, but ONLY ONCE per row
    if (this.activeRowDrainer.isReadyToYield() && !this.activeRowDrainer.isRowEmitted()) {
      TdsRow row = this.activeRowDrainer.assembleRow();

      // Hand the pause/resume power directly to the Row
      row.setAsyncCallbacks(
          () -> isPaused.set(true),
          () -> {
            isPaused.set(false);
            scheduleDrain(); // Wake up when Clob is done
          }
      );

      emitSegment(row); // User's map() executes here
      this.activeRowDrainer.setRowEmitted(true); // Mark as emitted so we don't duplicate
    }

    // PHASE 2: Discard drainer ONLY when the final column completely finishes over the network
    if (this.activeRowDrainer.isFullyComplete()) {
      this.activeRowDrainer = null;
    }
  }

  private void emitSegment(Result.Segment segment) {
    // Flip the flag so the drain loop knows an emission occurred
    this.segmentEmitted = true;

    try {
      if (onNext != null) {
        onNext.accept(segment);
      }
    } catch (Throwable t) {
      pushError(t);
      throw t;
    }
  }

  private void pushError(Throwable error) {
    logger.debug("Stream Error: {}", error.getMessage());
    if (onError != null) {
      onError.accept(error);
    }
  }

  private void pushComplete() {
    if (onComplete != null) {
      onComplete.run();
    }
  }
}
