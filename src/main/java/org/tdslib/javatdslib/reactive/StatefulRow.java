package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Row;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.tdslib.javatdslib.internal.TdsColumnMetadata;
import org.tdslib.javatdslib.internal.TdsRowMetadata;
import org.tdslib.javatdslib.protocol.CollationUtils;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.tokens.parsers.DataParser;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;
import org.tdslib.javatdslib.tokens.StatefulTokenDecoder;
import org.tdslib.javatdslib.codec.DecoderRegistry;

public class StatefulRow implements Row {
  private ByteBuffer payload;
  private final ColMetaDataToken metaData;
  private final TdsTransport transport;
  private final StatefulTokenDecoder decoder;
  private final ConnectionContext context;

  private int cursorIndex = 0;
  private final Object[] rowCache;
  private boolean lobActive = false;

  public StatefulRow(ByteBuffer payload, ColMetaDataToken metaData, TdsTransport transport, StatefulTokenDecoder decoder, ConnectionContext context) {
    this.payload = payload;
    this.metaData = metaData;
    this.transport = transport;
    this.decoder = decoder;
    this.context = context;
    this.rowCache = new Object[metaData.getColumns().size()];
  }

  public void resumeRowParsing(ByteBuffer unconsumedBytes) {
    if (unconsumedBytes != null && unconsumedBytes.hasRemaining()) {
      this.payload = unconsumedBytes;
    }
    this.lobActive = false;
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
      TdsType tdsType = TdsType.valueOf(col.getDataType());

      Charset charset = java.nio.charset.StandardCharsets.UTF_16LE;
      if (tdsType == TdsType.BIGVARCHR || tdsType == TdsType.VARCHAR || tdsType == TdsType.TEXT) {
        byte[] collation = col.getTypeInfo() != null ? col.getTypeInfo().getCollation() : null;
        charset = collation != null ? CollationUtils.getCharsetFromCollation(collation).orElse(context.getVarcharCharset()) : context.getVarcharCharset();
      }

      Object data = DataParser.getDataBytes(payload, tdsType, col.getMaxLength(), transport, decoder, charset);
      rowCache[cursorIndex] = data;

      if (data instanceof org.tdslib.javatdslib.streaming.TdsBlob) {
        ((org.tdslib.javatdslib.streaming.TdsBlob) data).setCompletionListener(this::resumeRowParsing);
        lobActive = true;
      } else if (data instanceof org.tdslib.javatdslib.streaming.TdsClob) {
        ((org.tdslib.javatdslib.streaming.TdsClob) data).setCompletionListener(this::resumeRowParsing);
        lobActive = true;
      }

      cursorIndex++;

      // --- NEW FIX: THE ESCAPE HATCH ---
      // If we just parsed the very last column, hand the trailing bytes back to the main decoder loop!
      if (cursorIndex == metaData.getColumns().size()) {
        if (payload != null && payload.hasRemaining()) {
          ByteBuffer trailingBytes = payload.slice();
          payload.position(payload.limit()); // Consume our buffer
          System.out.println(">>> STATEFUL ROW: Pushing " + trailingBytes.remaining() + " trapped bytes back to the Token Decoder.");
          decoder.onPayloadAvailable(trailingBytes, true);
        }
      }
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
}