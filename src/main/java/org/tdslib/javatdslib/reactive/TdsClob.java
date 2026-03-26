package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Clob;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
import org.tdslib.javatdslib.reactive.events.TdsStreamEvent;
import org.tdslib.javatdslib.tokens.ColumnData;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.PartialDataColumn;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.Charset;
import java.util.concurrent.locks.LockSupport;

public class TdsClob implements Clob {
  private final TdsTokenQueue tokenQueue;
  private final int columnIndex;
  private final Charset charset;
  private final Runnable rowUnlockCallback;

  private ColumnData firstChunk;
  private boolean isDiscardedOrCompleted = false;

  public TdsClob(TdsTokenQueue tokenQueue, int columnIndex, Charset charset, ColumnData firstChunk, Runnable rowUnlockCallback) {
    this.tokenQueue = tokenQueue;
    this.columnIndex = columnIndex;
    this.charset = charset;
    this.firstChunk = firstChunk;
    this.rowUnlockCallback = rowUnlockCallback;
  }

  @Override
  public Publisher<CharSequence> stream() {
    return Flux.<CharSequence>create(sink -> {
      if (isDiscardedOrCompleted) {
        sink.complete();
        return;
      }

      sink.onRequest(n -> {
        long emitted = 0;

        if (firstChunk != null && emitted < n && !sink.isCancelled()) {
          sink.next(decode(firstChunk));
          firstChunk = null;
          emitted++;
        }

        while (emitted < n && !sink.isCancelled() && !isDiscardedOrCompleted) {
          TdsStreamEvent event = tokenQueue.peek();

          if (event == null) {
            LockSupport.parkNanos(100_000);
            continue;
          }

          if (event instanceof ColumnEvent ce && ce.data().getColumnIndex() == columnIndex) {
            tokenQueue.poll();
            sink.next(decode(ce.data()));
            emitted++;
          } else {
            isDiscardedOrCompleted = true;
            rowUnlockCallback.run(); // WAKES UP THE SINK
            sink.complete();
            break;
          }
        }
      });

      sink.onCancel(this::syncDiscard);

    }).subscribeOn(Schedulers.boundedElastic());
  }

  @Override
  public Publisher<Void> discard() {
    return Mono.fromRunnable(this::syncDiscard).subscribeOn(Schedulers.boundedElastic()).then();
  }

  public void syncDiscard() {
    if (isDiscardedOrCompleted) return;

    while (true) {
      TdsStreamEvent event = tokenQueue.peek();
      if (event == null) {
        LockSupport.parkNanos(100_000);
        continue;
      }

      if (event instanceof ColumnEvent ce && ce.data().getColumnIndex() == columnIndex) {
        tokenQueue.poll();
      } else {
        break;
      }
    }

    isDiscardedOrCompleted = true;
    firstChunk = null;
    rowUnlockCallback.run(); // WAKES UP THE SINK
  }

  private String decode(ColumnData data) {
    if (data instanceof PartialDataColumn p && p.getChunk() != null) {
      return new String(p.getChunk(), charset);
    } else if (data instanceof CompleteDataColumn c && c.getData() != null) {
      return new String(c.getData(), charset);
    }
    return "";
  }
}