package org.tdslib.javatdslib.impl;

import io.r2dbc.spi.Clob;
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
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.concurrent.locks.LockSupport;

/**
 * An implementation of the R2DBC {@link Clob} interface, capable of streaming large
 * character payload data directly from the TDS stream.
 */
public class TdsClob implements Clob {
  private final TdsTokenQueue tokenQueue;
  private final int columnIndex;
  private final Charset charset;
  private final Runnable rowUnlockCallback;

  private ColumnData firstChunk;
  private boolean isDiscardedOrCompleted = false;

  // Production-ready stateful decoder to handle fragmented multibyte characters
  private final CharsetDecoder decoder;
  private byte[] leftoverBytes = null;

  /**
   * Creates a new instance of the {@code TdsClob}.
   *
   * @param tokenQueue        The token queue for reading chunks.
   * @param columnIndex       The column index of the CLOB data.
   * @param charset           The charset used to decode the CLOB data.
   * @param firstChunk        The initial chunk of data.
   * @param rowUnlockCallback Callback invoked when streaming completes or gets discarded.
   */
  public TdsClob(
      TdsTokenQueue tokenQueue, int columnIndex, Charset charset,
      ColumnData firstChunk, Runnable rowUnlockCallback) {
    this.tokenQueue = tokenQueue;
    this.columnIndex = columnIndex;
    this.charset = charset;
    this.firstChunk = firstChunk;
    this.rowUnlockCallback = rowUnlockCallback;

    // Initialize stateful decoder
    this.decoder = charset.newDecoder()
        .onMalformedInput(CodingErrorAction.REPLACE)
        .onUnmappableCharacter(CodingErrorAction.REPLACE);
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
          String decoded = decodeChunk(extractBytes(firstChunk));
          if (!decoded.isEmpty()) {
            sink.next(decoded);
            emitted++;
          }
          firstChunk = null;
        }

        while (emitted < n && !sink.isCancelled() && !isDiscardedOrCompleted) {
          TdsStreamEvent event = tokenQueue.peek();

          if (event == null) {
            LockSupport.parkNanos(100_000);
            continue;
          }

          if (event instanceof ColumnEvent ce && ce.data().getColumnIndex() == columnIndex) {
            tokenQueue.poll();
            String decoded = decodeChunk(extractBytes(ce.data()));
            if (!decoded.isEmpty()) {
              sink.next(decoded);
              emitted++;
            }
          } else {
            isDiscardedOrCompleted = true;

            // Flush decoder on final boundary to process any genuinely malformed trailing bytes
            String finalChars = flushDecoder();
            if (!finalChars.isEmpty()) {
              sink.next(finalChars);
            }

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

  /**
   * Called either by Publisher.discard(), Cancel, OR forcefully by TdsRow.
   */
  public void syncDiscard() {
    if (isDiscardedOrCompleted) {
      return;
    }

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
    leftoverBytes = null; // Clear state on discard
    rowUnlockCallback.run(); // WAKES UP THE SINK
  }

  private byte[] extractBytes(ColumnData data) {
    if (data instanceof PartialDataColumn p && p.getChunk() != null) {
      return p.getChunk();
    } else if (data instanceof CompleteDataColumn c && c.getData() != null) {
      return c.getData();
    }
    return new byte[0];
  }

  private String decodeChunk(byte[] rawBytes) {
    if ((rawBytes == null || rawBytes.length == 0) && leftoverBytes == null) {
      return "";
    }

    // 1. Merge any unread bytes from the previous chunk
    byte[] mergedBytes;
    if (leftoverBytes != null) {
      int rawLen = rawBytes != null ? rawBytes.length : 0;
      mergedBytes = new byte[leftoverBytes.length + rawLen];
      System.arraycopy(leftoverBytes, 0, mergedBytes, 0, leftoverBytes.length);
      if (rawLen > 0) {
        System.arraycopy(rawBytes, 0, mergedBytes, leftoverBytes.length, rawLen);
      }
      leftoverBytes = null;
    } else {
      mergedBytes = rawBytes;
    }

    if (mergedBytes.length == 0) {
      return "";
    }

    ByteBuffer in = ByteBuffer.wrap(mergedBytes);
    int maxChars = (int) Math.ceil(mergedBytes.length * decoder.maxCharsPerByte());
    CharBuffer out = CharBuffer.allocate(maxChars);

    // 2. Decode continuously (endOfInput = false preserves severed multibyte sequences)
    decoder.decode(in, out, false);

    // 3. Save any incomplete characters for the next chunk
    if (in.hasRemaining()) {
      leftoverBytes = new byte[in.remaining()];
      in.get(leftoverBytes);
    }

    out.flip();
    return out.toString();
  }

  private String flushDecoder() {
    ByteBuffer in = leftoverBytes != null ? ByteBuffer.wrap(leftoverBytes) : ByteBuffer.allocate(0);
    leftoverBytes = null;

    int maxChars = (int) Math.ceil(in.remaining() * decoder.maxCharsPerByte()) + 10;
    CharBuffer out = CharBuffer.allocate(maxChars);

    // Signal endOfInput = true to force processing of any genuinely malformed server data
    decoder.decode(in, out, true);
    decoder.flush(out);

    out.flip();
    return out.toString();
  }
}