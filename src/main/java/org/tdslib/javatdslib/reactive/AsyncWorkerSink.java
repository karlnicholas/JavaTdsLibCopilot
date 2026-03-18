package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Result;
import org.tdslib.javatdslib.internal.TdsUpdateCount;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
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

public class AsyncWorkerSink implements DataSink<TdsStreamEvent> {
  private final TdsEventPublisher publisher;
  private final ConnectionContext context; // Required for StatefulRow

  private final CountDownLatch completionLatch = new CountDownLatch(1);
  private final List<Result.Segment> receivedSegments = new CopyOnWriteArrayList<>();

  // --- Assembly State Machine ---
  private ColMetaDataToken activeMetaData;
  private byte[][] assemblingRow;

  public AsyncWorkerSink(TdsEventPublisher publisher, ConnectionContext context) {
    this.publisher = publisher;
    this.context = context;
  }

  @Override
  public void pushNext(TdsStreamEvent event) {
    // 1. Process the event to build rows
    if (event instanceof TokenEvent te) {
      processToken(te.token());
    } else if (event instanceof ColumnEvent ce) {
      processColumn(ce.data());
    }

    // 2. We continuously request the next raw event until the stream completes
    publisher.request(1);
  }

  @Override
  public void pushError(Throwable error) {
    System.err.println("[" + Thread.currentThread().getName() + "] Stream Error: " + error.getMessage());
    completionLatch.countDown();
  }

  @Override
  public void pushComplete() {
    System.out.println("[" + Thread.currentThread().getName() + "] Stream Complete!");
    completionLatch.countDown();
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
        publisher.setDownstreamSink(null); // Cleanup
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
      // Row is completely hydrated.
      final byte[][] finalRowData = this.assemblingRow;
      final ColMetaDataToken meta = this.activeMetaData;

      emitSegment(new StatefulRow(finalRowData, meta, context));

      this.assemblingRow = null;
    }
  }

  private void emitSegment(Result.Segment segment) {
    System.out.println("[" + Thread.currentThread().getName() + "] Assembled & Emitted Segment: " + segment.getClass().getSimpleName());
    receivedSegments.add(segment);
  }

  // --- Utilities ---

  public void awaitCompletion() throws InterruptedException {
    completionLatch.await();
  }

  public List<Result.Segment> getReceivedSegments() {
    return receivedSegments;
  }
}