package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.codec.DecoderRegistry;
import org.tdslib.javatdslib.internal.TdsUpdateCount;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
import org.tdslib.javatdslib.reactive.events.ErrorEvent;
import org.tdslib.javatdslib.reactive.events.TdsStreamEvent;
import org.tdslib.javatdslib.reactive.events.TokenEvent;
import org.tdslib.javatdslib.tokens.*;
import org.tdslib.javatdslib.tokens.models.*;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.io.ByteArrayOutputStream;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ReactiveResultAssembler implements TdsDecoderSink {
  private static final Logger logger = LoggerFactory.getLogger(ReactiveResultAssembler.class);

  // --- Network Throttling Configuration ---
  private static final int HIGH_WATERMARK = 5 * 1024 * 1024; // 5 MB
  private static final int LOW_WATERMARK = 1 * 1024 * 1024;  // 1 MB

  // --- Core Dependencies ---
  private final TdsTransport transport;
  private final ConnectionContext context;
  private final Executor workerExecutor; // NEW: The thread pool for decoding
  private DataSink<Result.Segment> downstreamSink;

  // --- Thread Boundary / Concurrency State ---
  private final Queue<TdsStreamEvent> networkQueue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger queueByteWeight = new AtomicInteger(0);
  private final AtomicBoolean isNetworkSuspended = new AtomicBoolean(false);

  private final AtomicLong demand = new AtomicLong(0);
  private final AtomicInteger wip = new AtomicInteger(0);
  private final AtomicBoolean isCancelled = new AtomicBoolean(false);

  // --- Worker Thread Assembly State ---
  private final Queue<Result.Segment> readySegments = new LinkedList<>();
  private ColMetaDataToken activeMetaData;
  private byte[][] assemblingRow; // Changed back to raw bytes for standard mapping

  public ReactiveResultAssembler(TdsTransport transport, ConnectionContext context, Executor workerExecutor) {
    this.transport = transport;
    this.context = context;
    this.workerExecutor = workerExecutor;
  }

  public void setDownstreamSink(DataSink<Result.Segment> downstreamSink) {
    this.downstreamSink = downstreamSink;
  }

  // ====================================================================================
  // NETWORK THREAD: SINK IMPLEMENTATION & THROTTLING
  // ====================================================================================

  @Override
  public void onToken(Token token) { offerEvent(new TokenEvent(token)); }

  @Override
  public void onColumnData(ColumnData data) { offerEvent(new ColumnEvent(data)); }

  @Override
  public void onError(Throwable error) { offerEvent(new ErrorEvent(error)); }

  private void offerEvent(TdsStreamEvent event) {
    networkQueue.offer(event);
    int currentWeight = queueByteWeight.addAndGet(event.getByteWeight());

    if (currentWeight > HIGH_WATERMARK && isNetworkSuspended.compareAndSet(false, true)) {
      logger.debug("High Watermark reached ({} bytes). Suspending network.", currentWeight);
      transport.suspendNetworkRead();
    }

    scheduleDrain();
  }

  public void request(long n) {
    if (n > 0) {
      demand.addAndGet(n);
      scheduleDrain();
    }
  }

  public void cancel() {
    isCancelled.set(true);
    transport.cancelCurrent();
  }

  // ====================================================================================
  // WORKER THREAD: DRAIN LOOP & STATEFUL ASSEMBLY
  // ====================================================================================

  private void scheduleDrain() {
    if (wip.getAndIncrement() == 0) {
      // NEW: Ensure the drain loop is dispatched to the worker thread
      if (workerExecutor != null) {
        workerExecutor.execute(this::drain);
      } else {
        drain(); // Fallback for synchronous tests
      }
    }
  }

  private void drain() {
    int missed = 1;
    do {
      long requested = demand.get();
      long emitted = 0;

      while (emitted != requested) {
        if (isCancelled.get()) {
          networkQueue.clear();
          return;
        }

        if (!readySegments.isEmpty()) {
          downstreamSink.pushNext(readySegments.poll());
          emitted++;
          continue;
        }

        TdsStreamEvent event = networkQueue.poll();
        if (event == null) break;

        int weight = queueByteWeight.addAndGet(-event.getByteWeight());
        if (weight < LOW_WATERMARK && isNetworkSuspended.compareAndSet(true, false)) {
          logger.debug("Low Watermark reached ({} bytes). Resuming network.", weight);
          transport.resumeNetworkRead();
        }

        if (event instanceof ErrorEvent err) {
          downstreamSink.pushError(err.error());
          return;
        } else if (event instanceof TokenEvent te) {
          processToken(te.token());
        } else if (event instanceof ColumnEvent ce) {
          processColumn(ce.data());
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
      this.assemblingRow = new byte[activeMetaData.getColumns().size()][];
    } else if (token instanceof DoneToken done) {
      if (done.getStatus().hasCount()) {
        readySegments.offer(new TdsUpdateCount(done.getCount()));
      }
      if (!done.getStatus().hasMoreResults()) {
        downstreamSink.pushComplete();
      }
    }
  }

  private void processColumn(ColumnData cd) {
    if (this.assemblingRow == null) return; // Defensive guard

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

//      readySegments.offer((Result.RowSegment) () -> new StatefulRow(finalRowData, meta, context));
      readySegments.offer(new StatefulRow(finalRowData, meta, context));
      this.assemblingRow = null;
    }
  }
}