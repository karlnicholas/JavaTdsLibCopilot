package org.tdslib.javatdslib.impl;

import io.r2dbc.spi.Blob;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.reactive.TdsTokenQueue;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
import org.tdslib.javatdslib.reactive.events.TdsStreamEvent;
import org.tdslib.javatdslib.tokens.ColumnData;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.PartialDataColumn;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;

/**
 * An implementation of the R2DBC {@link Blob} interface, capable of streaming large
 * binary payload data directly from the TDS stream.
 */
public class TdsBlob implements Blob {
  private final TdsTokenQueue tokenQueue;
  private final int columnIndex;
  private final Runnable rowUnlockCallback; // Called when stream completes or discards

  private ColumnData firstChunk; // Holds the chunk passed from RowDrainer
  private boolean isDiscardedOrCompleted = false;

  /**
   * Creates a new instance of the {@code TdsBlob}.
   *
   * @param tokenQueue        The token queue for reading chunks.
   * @param columnIndex       The column index of the BLOB data.
   * @param firstChunk        The initial chunk of data.
   * @param rowUnlockCallback Callback invoked when streaming completes or gets discarded.
   */
  public TdsBlob(
      TdsTokenQueue tokenQueue, int columnIndex, ColumnData firstChunk, Runnable rowUnlockCallback) {
    this.tokenQueue = tokenQueue;
    this.columnIndex = columnIndex;
    this.firstChunk = firstChunk;
    this.rowUnlockCallback = rowUnlockCallback;
  }

  @Override
  public Publisher<ByteBuffer> stream() {
    return Flux.<ByteBuffer>create(sink -> {
      if (isDiscardedOrCompleted) {
        sink.complete();
        return;
      }

      // Backpressure-aware polling
      sink.onRequest(n -> {
        long emitted = 0;

        // 1. Emit the first cached chunk immediately if available
        if (firstChunk != null && emitted < n && !sink.isCancelled()) {
          sink.next(decode(firstChunk));
          firstChunk = null;
          emitted++;
        }

        // 2. Poll the queue for the remaining chunks based on demand
        while (emitted < n && !sink.isCancelled() && !isDiscardedOrCompleted) {
          TdsStreamEvent event = tokenQueue.peek();

          if (event == null) {
            LockSupport.parkNanos(100_000); // Wait for NIO thread to push data
            continue;
          }

          if (event instanceof ColumnEvent ce && ce.data().getColumnIndex() == columnIndex) {
            // It's our chunk! Consume and emit.
            tokenQueue.poll();
            sink.next(decode(ce.data()));
            emitted++;
          } else {
            // Boundary reached (next column, ErrorEvent, or End of Row Token)
            isDiscardedOrCompleted = true;
            rowUnlockCallback.run(); // WAKES UP THE SINK
            sink.complete();
            break;
          }
        }
      });

      sink.onCancel(this::syncDiscard);

    }).subscribeOn(Schedulers.boundedElastic()); // CRITICAL: Protects the Netty thread from parkNanos
  }

  @Override
  public Publisher<Void> discard() {
    return Mono.fromRunnable(this::syncDiscard).subscribeOn(Schedulers.boundedElastic()).then();
  }

  /**
   * Called either by Publisher.discard(), Cancel, OR forcefully by TdsRow.
   */
  public void syncDiscard() {
    if (isDiscardedOrCompleted) {
      return;
    }

    // Fast-forward the queue until we hit the next column boundary
    while (true) {
      TdsStreamEvent event = tokenQueue.peek();
      if (event == null) {
        LockSupport.parkNanos(100_000);
        continue;
      }

      if (event instanceof ColumnEvent ce && ce.data().getColumnIndex() == columnIndex) {
        tokenQueue.poll(); // Drop the chunk
      } else {
        break; // Hit the boundary (do NOT consume the boundary event!)
      }
    }

    isDiscardedOrCompleted = true;
    firstChunk = null; // Free memory
    rowUnlockCallback.run(); // WAKES UP THE SINK
  }

  private ByteBuffer decode(ColumnData data) {
    if (data instanceof PartialDataColumn p && p.getChunk() != null) {
      return ByteBuffer.wrap(p.getChunk());
    } else if (data instanceof CompleteDataColumn c && c.getData() != null) {
      return ByteBuffer.wrap(c.getData());
    }
    return ByteBuffer.allocate(0); // Safe fallback for null payloads
  }
}