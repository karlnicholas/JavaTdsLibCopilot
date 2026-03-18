package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.internal.TdsUpdateCount;
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
  private byte[][] assemblingRow;
  private int activePlpIndex = -1;
  private final ByteArrayOutputStream plpAccumulator = new ByteArrayOutputStream();

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
    do {
      long requested = demand.get();
      long emitted = 0;

      while (emitted != requested) {
        if (isCancelled.get()) return;

        // Track how many segments we have before polling the next token
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

        // Only increment 'emitted' if a complete Row or UpdateCount was produced
        if (receivedSegments.size() > segmentsBefore) {
          emitted++;
        }
      }

      if (emitted != 0 && requested != Long.MAX_VALUE) {
        demand.addAndGet(-emitted);
      }
      missed = wip.addAndGet(-missed);
    } while (missed != 0);
  }

  private void processToken(Token token) {
    if (token instanceof ColMetaDataToken meta) {
      this.activeMetaData = meta;
    } else if (token instanceof RowToken) {
      flushPlpIfNecessary(-1);
      this.assemblingRow = new byte[activeMetaData.getColumns().size()][];
    } else if (token instanceof DoneToken done) {
      flushPlpIfNecessary(-1);
      if (done.getStatus().hasCount()) {
        emitSegment(new TdsUpdateCount(done.getCount()));
      }
      if (!done.getStatus().hasMoreResults()) {
        pushComplete();
      }
    }
  }

  private void processColumn(ColumnData cd) {
    if (this.assemblingRow == null) return;
    int colIndex = cd.getColumnIndex();
    flushPlpIfNecessary(colIndex);

    if (cd instanceof PartialDataColumn p) {
      activePlpIndex = colIndex;
      if (p.getChunk() != null) {
        plpAccumulator.write(p.getChunk(), 0, p.getChunk().length);
      }
    } else if (cd instanceof CompleteDataColumn c) {
      assemblingRow[colIndex] = c.getData();
      checkRowCompletion(colIndex);
    }
  }

  private void flushPlpIfNecessary(int incomingColIndex) {
    if (activePlpIndex != -1 && activePlpIndex != incomingColIndex) {
      byte[] fullPlpData = plpAccumulator.toByteArray();
      assemblingRow[activePlpIndex] = fullPlpData;
      plpAccumulator.reset();
      int finishedIndex = activePlpIndex;
      activePlpIndex = -1;
      checkRowCompletion(finishedIndex);
    }
  }

  private void checkRowCompletion(int justFinishedColIndex) {
    if (activeMetaData != null && justFinishedColIndex == activeMetaData.getColumns().size() - 1) {
      emitSegment(new StatefulRow(this.assemblingRow, this.activeMetaData, context));
      this.assemblingRow = null;
    }
  }

  private void emitSegment(Result.Segment segment) {
    logger.trace("Assembled Segment: {}", segment.getClass().getSimpleName());
    receivedSegments.add(segment);
    if (onNext != null) onNext.accept(segment);
  }

  private void pushError(Throwable error) {
    logger.error("Stream Error: {}", error.getMessage(), error);
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