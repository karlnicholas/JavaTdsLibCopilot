package org.tdslib.javatdslib.impl;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.codec.DecoderRegistry;
import org.tdslib.javatdslib.protocol.CollationUtils;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.reactive.RowDrainer;
import org.tdslib.javatdslib.reactive.TdsTokenQueue;
import org.tdslib.javatdslib.reactive.events.ColumnEvent;
import org.tdslib.javatdslib.reactive.events.ErrorEvent;
import org.tdslib.javatdslib.reactive.events.TdsStreamEvent;
import org.tdslib.javatdslib.reactive.events.TokenEvent;
import org.tdslib.javatdslib.tokens.ColumnData;
import org.tdslib.javatdslib.tokens.CompleteDataColumn;
import org.tdslib.javatdslib.tokens.PartialDataColumn;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * A highly performant, random-access Row and RowSegment implementation.
 * Modified to support hybrid materialized and deferred payload models.
 */
public class TdsRow implements Row, Result.RowSegment {
  private static final Logger logger = LoggerFactory.getLogger(TdsRow.class);

  // NEW: State marker for permanent network drops
  private static final Object DISCARDED = new Object();

  private Runnable pauseSinkCallback;
  private Runnable resumeSinkCallback;

  // Reference counter to safely manage concurrent LOB extraction
  private final AtomicInteger pendingLobCount = new AtomicInteger(0);

  /**
   * Sets callbacks to pause and resume the network sink.
   *
   * @param pauseCallback  Runnable to pause the sink.
   * @param resumeCallback Runnable to resume the sink.
   */
  public void setAsyncCallbacks(Runnable pauseCallback, Runnable resumeCallback) {
    this.pauseSinkCallback = pauseCallback;
    this.resumeSinkCallback = resumeCallback;
  }

  private final Object[] payload;
  private final ColMetaDataToken metaData;
  private final ConnectionContext context;
  private final TdsRowMetadata rowMetadata;
  private final TdsTokenQueue tokenQueue;

  /**
   * Creates a new TdsRow instance.
   *
   * @param payload    The row payload data array.
   * @param metaData   The column metadata token.
   * @param context    The connection context.
   * @param tokenQueue The token queue.
   */
  public TdsRow(
      Object[] payload, ColMetaDataToken metaData,
      ConnectionContext context, TdsTokenQueue tokenQueue) {
    this.payload = payload;
    this.metaData = metaData;
    this.context = context;
    this.tokenQueue = tokenQueue;

    // Cache the R2DBC Metadata once upon creation to optimize Result.map() operations
    List<ColumnMetadata> columns = new ArrayList<>(metaData.getColumns().size());
    for (ColumnMeta meta : metaData.getColumns()) {
      columns.add(new TdsColumnMetadata(meta));
    }
    this.rowMetadata = new TdsRowMetadata(columns);
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    for (int i = 0; i < metaData.getColumns().size(); i++) {
      if (metaData.getColumns().get(i).getName().equalsIgnoreCase(name)) {
        return get(i, type);
      }
    }
    logger.debug("[TdsRow] Column name '{}' not found in row metadata.", name);
    throw new IllegalArgumentException("Column not found: " + name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int index, Class<T> type) {
    if (index < 0 || index >= payload.length) {
      throw new IllegalArgumentException("Invalid Column Index: " + index);
    }

    Object rawData = payload[index];

    // NEW: Block access if the network queue passed this column without reading it
    if (rawData == DISCARDED) {
      throw new IllegalStateException(String.format(
          "Forward-only violation. Column %d has already been consumed or was skipped.", index));
    }

    ColumnMeta colMeta = metaData.getColumns().get(index);
    TdsType tdsType = TdsType.valueOf(colMeta.getDataType());

    // --- 1. Intercept Asynchronous Streams (LOBs) ---
    if (type == Clob.class || type == Blob.class) {
      if (rawData == RowDrainer.UNFETCHED) {
        rawData = advanceQueueToColumn(index);
        discardUnfetchedColumnsBefore(index);
      } else if (rawData instanceof byte[] bytes) {
        // Edge case: User asked for a Clob on a tiny string already fully in memory
        rawData = new CompleteDataColumn(index, bytes);
      }

      payload[index] = DISCARDED; // LOB Streams can only be consumed once!
      discardUnfetchedColumnsBefore(index);

      pendingLobCount.incrementAndGet();
      if (this.pauseSinkCallback != null) {
        this.pauseSinkCallback.run();
      }

      Runnable mediatedResumeCallback = () -> {
        if (pendingLobCount.decrementAndGet() == 0) {
          logger.trace("[TdsRow] All pending LOBs completed. Resuming network sink.");
          if (this.resumeSinkCallback != null) {
            this.resumeSinkCallback.run();
          }
        }
      };

      ColumnData initialChunk = (rawData instanceof ColumnData cd) ? cd : null;
      if (type == Clob.class) {
        Charset charset = getCharset(colMeta, tdsType);
        return type.cast(new TdsClob(
            tokenQueue, index, charset, initialChunk, mediatedResumeCallback));
      } else {
        return type.cast(new TdsBlob(tokenQueue, index, initialChunk, mediatedResumeCallback));
      }
    }

    // --- 2. On-Demand Network Fetching for Standard Columns ---
    if (rawData == RowDrainer.UNFETCHED) {
      rawData = advanceQueueToColumn(index);
      discardUnfetchedColumnsBefore(index);

      // If it's a completely fetched standard column, unwrap it to byte[]
      // so it can be cached and reused
      if (rawData instanceof CompleteDataColumn c) {
        if (!isPlp(tdsType, colMeta)) {
          rawData = c.getData();
          payload[index] = rawData; // Cache it in memory!
        }
      }
    }

    // --- 3. Process the Extracted Data ---

    if (rawData == null) {
      return null;
    }

    if (rawData instanceof ColumnData chunk) {
      payload[index] = DISCARDED; // Consuming a LOB synchronously permanently consumes it
      return (T) drainLobSynchronously(index, type, tdsType, colMeta, chunk);
    }

    if (rawData instanceof byte[] bytes) {
      Charset charset = getCharset(colMeta, tdsType);
      // NOTE: We do NOT discard it here! The byte array is safely in memory.
      // The user can read this column natively as many times as they want.
      return DecoderRegistry.DEFAULT.decode(bytes, tdsType, type, colMeta.getScale(), charset);
    }

    throw new IllegalStateException("Unknown payload type: " + rawData.getClass().getName());
  }

  /**
   * Fast-forwards the network queue to the requested column.
   */
  private ColumnData advanceQueueToColumn(int targetIndex) {
    while (true) {
      TdsStreamEvent event = tokenQueue.peek();
      if (event == null) {
        LockSupport.parkNanos(100_000);
        continue;
      }
      if (event instanceof ColumnEvent ce) {
        if (ce.data().getColumnIndex() < targetIndex) {
          tokenQueue.poll(); // User skipped this column. Discard its chunk.
        } else if (ce.data().getColumnIndex() == targetIndex) {
          return ((ColumnEvent) tokenQueue.poll()).data(); // Found it! Consume and return.
        } else {
          throw new IllegalStateException(
              "Desync: Expected col " + targetIndex + " but got " + ce.data().getColumnIndex());
        }
      } else if (event instanceof TokenEvent) {
        return null; // End of row reached without finding the column
      } else if (event instanceof ErrorEvent ee) {
        tokenQueue.poll();
        throw new RuntimeException("Server Error", ee.error());
      }
    }
  }

  /**
   * Drops memory references to previous unread streaming columns to prevent leaks.
   */
  private void discardUnfetchedColumnsBefore(int index) {
    for (int i = 0; i < index; i++) {
      if (payload[i] == RowDrainer.UNFETCHED || payload[i] instanceof ColumnData) {
        payload[i] = DISCARDED;
      }
    }
  }

  private boolean isPlp(TdsType tdsType, ColumnMeta colMeta) {
    if (tdsType == null) {
      return false;
    }
    return tdsType.strategy == TdsType.LengthStrategy.PLP
        || (tdsType.strategy == TdsType.LengthStrategy.USHORTLEN
        && colMeta.getMaxLength() == 65535);
  }

  private Object drainLobSynchronously(
      int index, Class<?> type, TdsType tdsType, ColumnMeta colMeta, ColumnData firstChunk) {
    logger.trace("[TdsRow] Initiating Synchronous LOB Drain for column {}", index);
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    boolean isNullData = false;

    try {
      if (firstChunk instanceof PartialDataColumn p) {
        if (p.getChunk() != null) {
          buffer.write(p.getChunk());
        } else {
          isNullData = true;
        }
      } else if (firstChunk instanceof CompleteDataColumn c) {
        if (c.getData() != null) {
          buffer.write(c.getData());
        } else {
          isNullData = true;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    while (true) {
      TdsStreamEvent event = tokenQueue.peek();
      if (event == null) {
        LockSupport.parkNanos(100_000);
        continue;
      }

      if (event instanceof ColumnEvent ce) {
        if (ce.data().getColumnIndex() == index) {
          tokenQueue.poll();
          try {
            if (ce.data() instanceof PartialDataColumn p && p.getChunk() != null) {
              buffer.write(p.getChunk());
            } else if (ce.data() instanceof CompleteDataColumn c && c.getData() != null) {
              buffer.write(c.getData());
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        } else {
          break; // Next column boundary Reached!
        }
      } else if (event instanceof org.tdslib.javatdslib.reactive.events.TokenEvent) {
        break; // End of row boundary Reached!
      } else if (event instanceof ErrorEvent ee) {
        tokenQueue.poll();
        throw new RuntimeException("Server Error during LOB streaming", ee.error());
      } else {
        throw new IllegalStateException("Unexpected event: " + event.getClass().getSimpleName());
      }
    }

    byte[] rawBytes = buffer.toByteArray();
    if (rawBytes.length == 0 && isNullData) {
      return null;
    }

    Charset charset = getCharset(colMeta, tdsType);

    try {
      return DecoderRegistry.DEFAULT.decode(rawBytes, tdsType, type, colMeta.getScale(), charset);
    } catch (OutOfMemoryError oom) {
      rawBytes = null;
      buffer = null;
      throw new IllegalStateException(
          "Driver ran out of memory materializing a large object. "
              + "Consider using streaming (Publisher.class) instead of synchronous get.", oom);
    }
  }

  private Charset getCharset(ColumnMeta colMeta, TdsType tdsType) {
    if (tdsType == TdsType.NVARCHAR || tdsType == TdsType.NCHAR
        || tdsType == TdsType.NTEXT || tdsType == TdsType.XML) {
      return java.nio.charset.StandardCharsets.UTF_16LE;
    } else {
      byte[] collation = colMeta.getTypeInfo() != null
          ? colMeta.getTypeInfo().getCollation()
          : null;
      return collation != null
          ? CollationUtils.getCharsetFromCollation(collation).orElse(context.getVarcharCharset())
          : context.getVarcharCharset();
    }
  }

  @Override
  public RowMetadata getMetadata() {
    return this.rowMetadata;
  }

  @Override
  public Row row() {
    return this;
  }

  @Override
  public String toString() {
    return "Result.RowSegment";
  }

  public Object[] getRowData() {
    return payload;
  }
}
