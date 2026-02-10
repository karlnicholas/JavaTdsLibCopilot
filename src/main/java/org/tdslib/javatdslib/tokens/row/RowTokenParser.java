package org.tdslib.javatdslib.tokens.row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsType;
import org.tdslib.javatdslib.tokens.DataParser;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RowTokenParser implements TokenParser {

  private static final Logger log = LoggerFactory.getLogger(RowTokenParser.class);

  @Override
  public Token parse(final ByteBuffer payload, final byte tokenType,
                     final ConnectionContext context, final QueryContext queryContext) {
    if (tokenType != (byte) 0xD1) {
      throw new IllegalArgumentException("Expected ROW (0xD1), got 0x" + Integer.toHexString(tokenType & 0xFF));
    }

    final List<byte[]> columnData = new ArrayList<>();
    final List<ColumnMeta> columns = queryContext.getColMetaDataToken().getColumns();

    for (final ColumnMeta col : columns) {
      // Convert byte back to our smart Enum
      TdsType type = TdsType.valueOf(col.getDataType());
      log.trace("col {} DataType {}", col, type);
      byte[] data = DataParser.getDataBytes(payload, type, col.getMaxLength());
      columnData.add(data);
    }
    return new RowToken(tokenType, columnData);
  }


}