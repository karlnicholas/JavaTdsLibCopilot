package org.tdslib.javatdslib.reactive;

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
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * A highly performant, random-access Row and RowSegment implementation.
 * Operates on a fully materialized byte[][] payload assembled by the AsyncWorkerSink.
 */
public class StatefulRow implements Row, Result.RowSegment {
  private static final Logger logger = LoggerFactory.getLogger(StatefulRow.class);

  private final byte[][] payload;
  private final ColMetaDataToken metaData;
  private final ConnectionContext context;
  private final TdsRowMetadata rowMetadata;

  public StatefulRow(byte[][] payload, ColMetaDataToken metaData, ConnectionContext context) {
    this.payload = payload;
    this.metaData = metaData;
    this.context = context;

    // Cache the R2DBC Metadata once upon creation to optimize Result.map() operations
    List<ColumnMetadata> columns = new ArrayList<>(metaData.getColumns().size());
    for (ColumnMeta meta : metaData.getColumns()) {
      columns.add(new TdsColumnMetadata(meta));
    }
    this.rowMetadata = new TdsRowMetadata(columns);
  }

  @Override
  public <T> T get(int index, Class<T> type) {
    if (index < 0 || index >= payload.length) {
      logger.debug("[StatefulRow] Out of bounds access attempt: index {} for row with {} columns", index, payload.length);
      throw new IllegalArgumentException("Invalid Column Index: " + index);
    }

    byte[] rawData = payload[index];
    if (rawData == null) {
      logger.trace("[StatefulRow] Column {} is NULL", index);
      return null;
    }

    ColumnMeta colMeta = metaData.getColumns().get(index);
    TdsType tdsType = TdsType.valueOf(colMeta.getDataType());

    // Everything else (CHAR, VARCHAR, TEXT) uses the server collation.
    Charset charset;
    if (tdsType == TdsType.NVARCHAR || tdsType == TdsType.NCHAR || tdsType == TdsType.NTEXT) {
      charset = java.nio.charset.StandardCharsets.UTF_16LE;
    } else {
      byte[] collation = colMeta.getTypeInfo() != null ? colMeta.getTypeInfo().getCollation() : null;
      charset = collation != null
          ? CollationUtils.getCharsetFromCollation(collation).orElse(context.getVarcharCharset())
          : context.getVarcharCharset();
    }

    // Decode directly from the byte[] into the requested Java type
    T decoded = DecoderRegistry.DEFAULT.decode(rawData, tdsType, type, colMeta.getScale(), charset);
    logger.trace("[StatefulRow] Decoding column {}: Type={}, Target={}, Charset={}, Bytes={}, Value={}",
        index, tdsType, type.getSimpleName(), charset, rawData.length, decoded);
    return decoded;
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
  public RowMetadata getMetadata() {
    return this.rowMetadata;
  }

  @Override
  public Row row() {
    // StatefulRow implements Result.RowSegment natively to prevent object wrapping
    return this;
  }

  @Override
  public String toString() {
    return "Result.RowSegment";
  }

  /**
   * Internal hook for Integration Testing
   */
  public byte[][] getRowData() {
    return payload;
  }
}