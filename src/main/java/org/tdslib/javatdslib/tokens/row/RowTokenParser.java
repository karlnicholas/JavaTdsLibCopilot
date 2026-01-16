package org.tdslib.javatdslib.tokens.row;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses standard ROW token (0xD1).
 *
 * <p>Assumes we have previous COL_METADATA to know types/lengths. For simplicity,
 * reads all columns as byte[] (raw data).</p>
 */
public class RowTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload,
                     final byte tokenType,
                     final ConnectionContext context,
                     final QueryContext queryContext) {
    if (tokenType != (byte) 0xD1) {
      final String hex = Integer.toHexString(tokenType & 0xFF);
      throw new IllegalArgumentException("Expected ROW (0xD1), got 0x" + hex);
    }

    final List<byte[]> columnData = new ArrayList<>();

    // For each column from previous metadata
    for (final ColumnMeta col : queryContext.getColMetaDataToken().getColumns()) {
      final byte dataType = col.getDataType();
      final byte[] data;

      if (TdsDataTypes.isFixedLength(dataType)) {
        final int length = TdsDataTypes.getFixedLength(dataType);
        if (length > 0) {
          final byte[] buf = new byte[length];
          payload.get(buf);
          data = buf;
        } else {
          data = null; // Should not happen for fixed lengths
        }
      } else if (TdsDataTypes.isNullableFixedLength(dataType)) {
        // For nullable types like INTN (0x26), etc.: BYTE length prefix
        final int len = payload.get() & 0xFF;
        if (len == 0) {
          data = null; // NULL
        } else {
          final byte[] buf = new byte[len];
          payload.get(buf);
          data = buf;
        }
      } else {
        // Variable length: USHORT length prefix (0xFFFF = NULL)
        final int len = payload.getShort() & 0xFFFF;
        if (len == 0xFFFF) {
          data = null; // NULL
        } else {
          final byte[] buf = new byte[len];
          payload.get(buf);
          data = buf;
        }
      }

      columnData.add(data);
    }

    return new RowToken(tokenType, columnData);
  }
}
