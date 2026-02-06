package org.tdslib.javatdslib.tokens.row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsType;
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
      byte[] data = null;

      switch (type.strategy) {
        case FIXED:
          data = readBytes(payload, type.fixedSize);
          break;

        case BYTELEN: // INTN, BITN, etc.
          int len = payload.get() & 0xFF;
          if (len > 0) data = readBytes(payload, len);
          // else null
          break;

        case USHORTLEN: // NVARCHAR, VARBINARY
          int varLen = Short.toUnsignedInt(payload.getShort());
          if (varLen != 0xFFFF) data = readBytes(payload, varLen);
          // else null
          break;

        case PLP:
          data = readPlp(payload);
          break;

        // ... Handle specific edge cases like Text Pointers if necessary ...
      }
      columnData.add(data);
    }
    return new RowToken(tokenType, columnData);
  }

  private byte[] readBytes(ByteBuffer buf, int length) {
    byte[] b = new byte[length];
    buf.get(b);
    return b;
  }

  private byte[] readPlp(ByteBuffer payload) {
    long totalLength = payload.getLong();
    if (totalLength == -1L) return null;

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    while (true) {
      int chunkLen = payload.getInt();
      if (chunkLen == 0) break;
      byte[] chunk = new byte[chunkLen];
      payload.get(chunk);
      buffer.writeBytes(chunk);
    }
    return buffer.toByteArray();
  }
}