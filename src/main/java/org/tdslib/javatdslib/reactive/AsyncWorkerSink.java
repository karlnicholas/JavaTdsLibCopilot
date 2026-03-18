package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.tdslib.javatdslib.internal.TdsUpdateCount;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
import org.tdslib.javatdslib.reactive.events.ErrorEvent;
import org.tdslib.javatdslib.reactive.events.TdsStreamEvent;
import org.tdslib.javatdslib.reactive.events.TokenEvent;
import org.tdslib.javatdslib.tokens.ColumnData;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.DoneToken;
import org.tdslib.javatdslib.tokens.models.RowToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncWorkerSink {
  private final TdsTokenQueue tokenQueue;
  private final ConnectionContext context;
  private final Executor workerExecutor;

  // --- Reactive Demand State ---
  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicInteger wip = new AtomicInteger(0);
  private final AtomicBoolean isCancelled = new AtomicBoolean(false);

  // --- Test Output State ---
  private final CountDownLatch completionLatch = new CountDownLatch(1);
  private final List<Result.Segment> receivedSegments = new CopyOnWriteArrayList<>();

  // --- Row Assembly State ---
  private ColMetaDataToken activeMetaData;
  private byte[][] assemblingRow;

  public AsyncWorkerSink(TdsTokenQueue tokenQueue, ConnectionContext context, Executor workerExecutor) {
    this.tokenQueue = tokenQueue;
    this.context = context;
    this.workerExecutor = workerExecutor;

    // Wire up the queue's notification to our drain loop
    this.tokenQueue.setOnEventAvailableCallback(this::scheduleDrain);
  }

  // ====================================================================================
  // REACTIVE SUBSCRIBER CONTRACT
  // ====================================================================================

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

  // ====================================================================================
  // THREAD HANDOFF & DRAIN LOOP
  // ====================================================================================

  private void scheduleDrain() {
    if (wip.getAndIncrement() == 0) {
      if (workerExecutor != null) {
        workerExecutor.execute(this::drain);
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

        // Pull directly from the pure queue
        TdsStreamEvent event = tokenQueue.poll();
        if (event == null) break; // Queue is empty, wait for next notification

        if (event instanceof ErrorEvent err) {
          pushError(err.error());
          return;
        } else if (event instanceof TokenEvent te) {
          processToken(te.token());
        } else if (event instanceof ColumnEvent ce) {
          processColumn(ce.data());
        }

        // Note: We are counting raw events emitted against demand for the test.
        // In full R2DBC, demand applies to fully assembled Rows, not raw tokens.
        emitted++;
      }

      if (emitted != 0 && requested != Long.MAX_VALUE) {
        demand.addAndGet(-emitted);
      }

      missed = wip.addAndGet(-missed);
    } while (missed != 0);
  }

  // ====================================================================================
  // ROW ASSEMBLY STATE MACHINE
  // ====================================================================================

  private void processToken(Token token) {
    if (token instanceof ColMetaDataToken meta) {
      this.activeMetaData = meta;
    } else if (token instanceof RowToken) {
      this.assemblingRow = new byte[activeMetaData.getColumns().size()][];
    } else if (token instanceof DoneToken done) {
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
    if (cd instanceof CompleteDataColumn c) {
      assemblingRow[colIndex] = c.getData();
      checkRowCompletion(colIndex);
    }
  }

  private void checkRowCompletion(int justFinishedColIndex) {
    if (activeMetaData != null && justFinishedColIndex == activeMetaData.getColumns().size() - 1) {
      final byte[][] finalRowData = this.assemblingRow;
      final ColMetaDataToken meta = this.activeMetaData;

      emitSegment(new StatefulRow(finalRowData, meta, context));
      this.assemblingRow = null;
    }
  }

  private void emitSegment(Result.Segment segment) {
    System.out.println("[" + Thread.currentThread().getName() + "] Assembled Segment: " + segment.getClass().getSimpleName());
    receivedSegments.add(segment);
  }

  private void pushError(Throwable error) {
    System.err.println("[" + Thread.currentThread().getName() + "] Error: " + error.getMessage());
    completionLatch.countDown();
  }

  private void pushComplete() {
    System.out.println("[" + Thread.currentThread().getName() + "] Stream Complete!");
    completionLatch.countDown();
  }

  public void awaitCompletion() throws InterruptedException { completionLatch.await(); }
  public List<Result.Segment> getReceivedSegments() { return receivedSegments; }
}