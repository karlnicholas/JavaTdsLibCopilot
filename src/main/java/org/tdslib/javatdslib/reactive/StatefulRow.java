package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.codec.DecoderRegistry;
import org.tdslib.javatdslib.internal.TdsColumnMetadata;
import org.tdslib.javatdslib.internal.TdsRowMetadata;
import org.tdslib.javatdslib.protocol.CollationUtils;
import org.tdslib.javatdslib.protocol.TdsType;
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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * A highly performant, random-access Row and RowSegment implementation.
 * Modified to support hybrid materialized and deferred payload models.
 */
public class StatefulRow implements Row, Result.RowSegment {
  private static final Logger logger = LoggerFactory.getLogger(StatefulRow.class);
  private Runnable pauseSinkCallback;
  private Runnable resumeSinkCallback;

  // NEW: Reference counter to safely manage concurrent LOB extraction
  private final AtomicInteger pendingLobCount = new AtomicInteger(0);

  public void setAsyncCallbacks(Runnable pauseCallback, Runnable resumeCallback) {
    this.pauseSinkCallback = pauseCallback;
    this.resumeSinkCallback = resumeCallback;
  }

  private final Object[] payload; // Shifted from byte[][] to Object[]
  private final ColMetaDataToken metaData;
  private final ConnectionContext context;
  private final TdsRowMetadata rowMetadata;

  private int activeColumnIndex = 0; // State tracker for strict sequential access

  private final TdsTokenQueue tokenQueue;

  public StatefulRow(Object[] payload, ColMetaDataToken metaData, ConnectionContext context, TdsTokenQueue tokenQueue) {
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
    logger.debug("[StatefulRow] Column name '{}' not found in row metadata.", name);
    throw new IllegalArgumentException("Column not found: " + name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int index, Class<T> type) {

    if (index > activeColumnIndex) {
      throw new IllegalStateException(
          String.format("Forward-only violation. Expected col %d but requested %d.", activeColumnIndex, index));
    } else if (index < activeColumnIndex) {
      throw new IllegalStateException(
          String.format("Forward-only violation. Column %d has already been consumed.", index));
    }

    if (index < 0 || index >= payload.length) {
      throw new IllegalArgumentException("Invalid Column Index: " + index);
    }

    Object rawData = payload[index];
    activeColumnIndex++;

    ColumnMeta colMeta = metaData.getColumns().get(index);
    TdsType tdsType = TdsType.valueOf(colMeta.getDataType());

    // 1. Intercept Asynchronous Streams (LOBs) immediately before queue-advancement
    if (type == Clob.class || type == Blob.class) {
      ColumnData initialChunk = (rawData instanceof ColumnData cd) ? cd : null;

      // Increment the reference counter since a new LOB publisher is being created
      pendingLobCount.incrementAndGet();

      // Pause the sink unconditionally to protect the queue for the Publisher
      if (this.pauseSinkCallback != null) this.pauseSinkCallback.run();

      // NEW: A mediator callback that only wakes the sink when ALL requested LOB streams are finished
      Runnable mediatedResumeCallback = () -> {
        if (pendingLobCount.decrementAndGet() == 0) {
          logger.trace("[StatefulRow] All pending LOBs completed. Resuming network sink.");
          if (this.resumeSinkCallback != null) this.resumeSinkCallback.run();
        } else {
          logger.trace("[StatefulRow] LOB completed, but others remain pending. Sink stays paused.");
        }
      };

      if (type == Clob.class) {
        Charset charset = getCharset(colMeta, tdsType);
        return type.cast(new TdsClob(tokenQueue, index, charset, initialChunk, mediatedResumeCallback));
      } else {
        return type.cast(new TdsBlob(tokenQueue, index, initialChunk, mediatedResumeCallback));
      }
    }

    // 2. On-Demand Fetching for subsequent unfetched standard columns
    if (rawData == RowDrainer.UNFETCHED) {
      rawData = advanceQueueToColumn(index);
      payload[index] = rawData; // Cache it
    }

    if (rawData == null) {
      return null;
    }

    // 3. Intercept Saved Chunks (Synchronous LOB extraction via String/byte[])
    if (rawData instanceof ColumnData chunk) {
      boolean isChunked = (chunk instanceof PartialDataColumn) ||
          (tdsType != null && (tdsType.strategy == TdsType.LengthStrategy.PLP || tdsType.strategy == TdsType.LengthStrategy.LONGLEN));

      if (isChunked) {
        if (type == String.class || type == byte[].class || type == ByteBuffer.class || type == Object.class) {
          return (T) drainLobSynchronously(index, type, tdsType, colMeta, chunk);
        } else {
          throw new IllegalArgumentException("Cannot map chunked LOB data to requested type: " + type.getName());
        }
      } else {
        // It's a standard column that was fetched on-demand. Unpack it natively.
        if (chunk instanceof CompleteDataColumn c) {
          rawData = c.getData();
          if (rawData == null) return null;
        } else {
          throw new IllegalStateException("Unexpected PartialDataColumn for standard type");
        }
      }
    }

    // 4. Process Standard Materialized Columns
    if (rawData instanceof byte[] bytes) {
      Charset charset = getCharset(colMeta, tdsType);
      return DecoderRegistry.DEFAULT.decode(bytes, tdsType, type, colMeta.getScale(), charset);
    }

    throw new IllegalStateException("Unknown payload type: " + rawData.getClass().getName());
  }

  /**
   * Fast-forwards the network queue to the requested column.
   * Natively discards chunks for columns the user skipped, enforcing forward-only memory safety.
   */
  private ColumnData advanceQueueToColumn(int targetIndex) {
    while (true) {
      TdsStreamEvent event = tokenQueue.peek(); // PEEK, do not poll yet!
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
          throw new IllegalStateException("Desync: Expected col " + targetIndex + " but got " + ce.data().getColumnIndex());
        }
      } else if (event instanceof TokenEvent) {
        return null; // End of row reached without finding the column
      } else if (event instanceof ErrorEvent ee) {
        tokenQueue.poll();
        throw new RuntimeException("Server Error", ee.error());
      }
    }
  }

  private Object drainLobSynchronously(int index, Class<?> type, TdsType tdsType, ColumnMeta colMeta, ColumnData firstChunk) {
    logger.trace("[StatefulRow] Initiating Synchronous LOB Drain for column {}", index);
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    boolean isNullData = false;

    // 1. Process the saved first chunk
    try {
      if (firstChunk instanceof PartialDataColumn p) {
        if (p.getChunk() != null) buffer.write(p.getChunk());
        else isNullData = true;
      } else if (firstChunk instanceof CompleteDataColumn c) {
        if (c.getData() != null) buffer.write(c.getData());
        else isNullData = true;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // 2. Safely peek and drain remaining chunks
    while (true) {
      TdsStreamEvent event = tokenQueue.peek(); // PEEK to detect boundaries
      if (event == null) {
        LockSupport.parkNanos(100_000);
        continue;
      }

      if (event instanceof ColumnEvent ce) {
        if (ce.data().getColumnIndex() == index) {
          // It's another chunk for our LOB. Consume it!
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
          // Boundary Reached! Next column found.
          break;
        }
      } else if (event instanceof org.tdslib.javatdslib.reactive.events.TokenEvent) {
        // Boundary Reached! End of row.
        break;
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
          "Driver ran out of memory materializing a large object. " +
              "Consider using streaming (Publisher.class) instead of synchronous get.", oom);
    }
  }

  private Charset getCharset(ColumnMeta colMeta, TdsType tdsType) {
    if (tdsType == TdsType.NVARCHAR || tdsType == TdsType.NCHAR || tdsType == TdsType.NTEXT || tdsType == TdsType.XML) {
      return java.nio.charset.StandardCharsets.UTF_16LE;
    } else {
      byte[] collation = colMeta.getTypeInfo() != null ? colMeta.getTypeInfo().getCollation() : null;
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

  /**
   * Internal hook for Integration Testing
   */
  public Object[] getRowData() { // Shifted return type to Object[]
    return payload;
  }
}