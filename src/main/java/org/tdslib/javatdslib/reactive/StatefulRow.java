package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Row;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.internal.TdsColumnMetadata;
import org.tdslib.javatdslib.internal.TdsRowMetadata;
import org.tdslib.javatdslib.protocol.CollationUtils;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.streaming.TdsBlob;
import org.tdslib.javatdslib.streaming.TdsClob;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.tokens.parsers.DataParser;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;
import org.tdslib.javatdslib.tokens.StatefulTokenDecoder;
import org.tdslib.javatdslib.codec.DecoderRegistry;

public class StatefulRow implements Row {
  private static final Logger logger = LoggerFactory.getLogger(StatefulRow.class);
  private ByteBuffer payload;
  private final ColMetaDataToken metaData;
  private final TdsTransport transport;
  private final StatefulTokenDecoder decoder;
  private final ConnectionContext context;

  private int cursorIndex = 0;
  private final Object[] rowCache;
  private boolean lobActive = false;
  private boolean isDraining = false;

  public StatefulRow(ByteBuffer payload, ColMetaDataToken metaData, TdsTransport transport, StatefulTokenDecoder decoder, ConnectionContext context) {
    this.payload = payload.order(ByteOrder.LITTLE_ENDIAN);
    this.metaData = metaData;
    this.transport = transport;
    this.decoder = decoder;
    this.context = context;
    this.rowCache = new Object[metaData.getColumns().size()];
  }

  public void drain() {
    if (lobActive) {
      logger.warn("[StatefulRow] Cannot synchronously drain row. A user LOB stream was abandoned while active.");
      return;
    }

    isDraining = true;
    logger.trace("[StatefulRow] Auto-Drain initiated. Fast-forwarding cursor from index {}", cursorIndex);

    while (cursorIndex < metaData.getColumns().size()) {
      ColumnMeta col = metaData.getColumns().get(cursorIndex);
      TdsType tdsType = TdsType.valueOf(col.getDataType());

      Charset charset = java.nio.charset.StandardCharsets.UTF_16LE;
      if (tdsType == TdsType.BIGVARCHR || tdsType == TdsType.VARCHAR || tdsType == TdsType.TEXT) {
        byte[] collation = col.getTypeInfo() != null ? col.getTypeInfo().getCollation() : null;
        charset = collation != null ? CollationUtils.getCharsetFromCollation(collation).orElse(context.getVarcharCharset()) : context.getVarcharCharset();
      }

      Object data = DataParser.getDataBytes(payload, tdsType, col.getMaxLength(), transport, decoder, charset);

      if (data instanceof TdsBlob tdsBlob) {
        logger.trace("[StatefulRow:Drain] Encountered unread BLOB. Instructing LOB to discard.");
        tdsBlob.setCompletionListener(this::resumeRowParsing);
        lobActive = true;
        tdsBlob.discard(); // The LOB will wake the network internally ONLY if it is starved
        return;
      } else if (data instanceof TdsClob tdsClob) {
        logger.trace("[StatefulRow:Drain] Encountered unread CLOB. Instructing LOB to discard.");
        tdsClob.setCompletionListener(this::resumeRowParsing);
        lobActive = true;
        tdsClob.discard(); // The LOB will wake the network internally ONLY if it is starved
        return;
      }
      cursorIndex++;
    }
    triggerEscapeHatchIfComplete();
  }

  public void resumeRowParsing(ByteBuffer unconsumedBytes) {
    logger.trace("[StatefulRow] LOB finished. Switching stream handler back to StatefulTokenDecoder.");
    transport.switchStreamHandler(decoder);

    if (unconsumedBytes != null && unconsumedBytes.hasRemaining()) {
      this.payload = unconsumedBytes.order(ByteOrder.LITTLE_ENDIAN);
    }
    this.lobActive = false;

    if (isDraining) {
      logger.trace("[StatefulRow] LOB discard complete. Resuming Auto-Drain loop.");
      drain();
    } else {
      triggerEscapeHatchIfComplete();
    }
  }

  @Override
  public <T> T get(int index, Class<T> type) {
    if (index < cursorIndex) {
      return decodeValue(rowCache[index], index, type);
    }

    if (lobActive) {
      throw new IllegalStateException("Cannot advance row cursor. A LOB stream is currently open.");
    }

    while (cursorIndex <= index) {
      ColumnMeta col = metaData.getColumns().get(cursorIndex);
      logger.trace("[StatefulRow] Decoding column {} ({})", cursorIndex, col.getName());
      TdsType tdsType = TdsType.valueOf(col.getDataType());

      Charset charset = java.nio.charset.StandardCharsets.UTF_16LE;
      if (tdsType == TdsType.BIGVARCHR || tdsType == TdsType.VARCHAR || tdsType == TdsType.TEXT) {
        byte[] collation = col.getTypeInfo() != null ? col.getTypeInfo().getCollation() : null;
        charset = collation != null ? CollationUtils.getCharsetFromCollation(collation).orElse(context.getVarcharCharset()) : context.getVarcharCharset();
      }

      Object data = DataParser.getDataBytes(payload, tdsType, col.getMaxLength(), transport, decoder, charset);

      if (data instanceof TdsBlob tdsBlob) {
        logger.trace("[StatefulRow] Column {} is a BLOB. Wrapping with proxy hook.", cursorIndex);
        tdsBlob.setCompletionListener(this::resumeRowParsing);
        lobActive = true;
        rowCache[cursorIndex] = new io.r2dbc.spi.Blob() {
          @Override public Publisher<ByteBuffer> stream() { return wrapPublisherWithResume(tdsBlob.stream()); }
          @Override public Publisher<Void> discard() { return createDiscardPublisher(tdsBlob::discard); }
        };
      } else if (data instanceof TdsClob tdsClob) {
        logger.trace("[StatefulRow] Column {} is a CLOB. Wrapping with proxy hook.", cursorIndex);
        tdsClob.setCompletionListener(this::resumeRowParsing);
        lobActive = true;
        rowCache[cursorIndex] = new io.r2dbc.spi.Clob() {
          @Override public Publisher<CharSequence> stream() { return wrapPublisherWithResume(tdsClob.stream()); }
          @Override public Publisher<Void> discard() { return createDiscardPublisher(tdsClob::discard); }
        };
      } else {
        logger.trace("[StatefulRow] Column {} cached as standard value.", cursorIndex);
        rowCache[cursorIndex] = data;
      }

      cursorIndex++;
      triggerEscapeHatchIfComplete();
    }

    return decodeValue(rowCache[index], index, type);
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    for (int i = 0; i < metaData.getColumns().size(); i++) {
      if (metaData.getColumns().get(i).getName().equalsIgnoreCase(name)) {
        return get(i, type);
      }
    }
    throw new IllegalArgumentException("Column not found: " + name);
  }

  @Override
  public io.r2dbc.spi.RowMetadata getMetadata() {
    List<ColumnMetadata> columns = new ArrayList<>();
    for (ColumnMeta meta : metaData.getColumns()) {
      columns.add(new TdsColumnMetadata(meta));
    }
    return new TdsRowMetadata(columns);
  }

  @SuppressWarnings("unchecked")
  private <T> T decodeValue(Object rawData, int index, Class<T> type) {
    if (rawData == null) return null;
    if (type.isInstance(rawData)) return (T) rawData;

    ColumnMeta colMeta = metaData.getColumns().get(index);
    TdsType tdsType = TdsType.valueOf(colMeta.getDataType());
    Charset charset = java.nio.charset.StandardCharsets.UTF_16LE;
    if (tdsType == TdsType.BIGVARCHR || tdsType == TdsType.VARCHAR || tdsType == TdsType.TEXT) {
      byte[] collation = colMeta.getTypeInfo() != null ? colMeta.getTypeInfo().getCollation() : null;
      charset = collation != null ? CollationUtils.getCharsetFromCollation(collation).orElse(context.getVarcharCharset()) : context.getVarcharCharset();
    }

    return DecoderRegistry.DEFAULT.decode((byte[]) rawData, tdsType, type, colMeta.getScale(), charset);
  }

  private void triggerEscapeHatchIfComplete() {
    if (cursorIndex == metaData.getColumns().size() && !lobActive) {
      if (payload != null && payload.hasRemaining()) {
        logger.trace("[StatefulRow] Escaping {} trailing bytes back to StatefulTokenDecoder.", payload.remaining());
        try {
          ByteBuffer trailingBytes = payload.slice().order(ByteOrder.LITTLE_ENDIAN);
          payload.position(payload.limit());
          decoder.onPayloadAvailable(trailingBytes, true);
        } catch (Exception e) {
          throw new IllegalStateException("Failed to parse trailing bytes after row", e);
        }
      }
      logger.trace("[StatefulRow] Row fully parsed. Turning EventLoop back on.");
      transport.resumeNetworkRead();
    }
  }

  private <P> Publisher<P> wrapPublisherWithResume(Publisher<P> source) {
    return subscriber -> source.subscribe(new Subscriber<P>() {
      @Override
      public void onSubscribe(Subscription s) {
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) {
            logger.trace("[StatefulRow:LOB-Wrapper] Client requested {}. Propagating to LOB without unconditional network wakeup.", n);
            s.request(n);
          }
          @Override
          public void cancel() {
            s.cancel();
          }
        });
      }
      @Override public void onNext(P item) { subscriber.onNext(item); }
      @Override public void onError(Throwable t) { subscriber.onError(t); }
      @Override public void onComplete() { subscriber.onComplete(); }
    });
  }

  private Publisher<Void> createDiscardPublisher(Runnable discardAction) {
    return subscriber -> subscriber.onSubscribe(new Subscription() {
      boolean executed = false;
      @Override
      public void request(long n) {
        if (n > 0 && !executed) {
          executed = true;
          try {
            logger.trace("[StatefulRow:LOB-Discard] Client manually discarding LOB.");
            discardAction.run();
            subscriber.onComplete();
          } catch (Exception e) {
            subscriber.onError(e);
          }
        }
      }
      @Override
      public void cancel() {}
    });
  }
}