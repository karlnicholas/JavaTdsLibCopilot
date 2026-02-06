package org.tdslib.javatdslib.tokens.colmetadata;

import io.r2dbc.spi.R2dbcType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsType;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ColMetaDataTokenParser implements TokenParser {
  private static Logger log = LoggerFactory.getLogger(ColMetaDataTokenParser.class);
  @Override
  public Token parse(final ByteBuffer payload, final byte tokenType,
                     final ConnectionContext context, final QueryContext queryContext) {
    if (tokenType != (byte) 0x81) {
      throw new IllegalArgumentException(
          "Expected COL_METADATA token (0x81), but got 0x"
              + Integer.toHexString(tokenType & 0xFF));
    }

    final short columnCount = payload.getShort();
    final List<ColumnMeta> columns = new ArrayList<>(columnCount);

    for (int colIndex = 0; colIndex < columnCount; colIndex++) {
      final int userType = payload.getInt();
      final short flags    = payload.getShort();

      final int dataType   = payload.get() & 0xFF; // Treat as unsigned int for switch
      log.trace("colIndex: {} userType: {} flags: {} dataType: {}", colIndex, userType, flags, dataType);
      int maxLength = -1;
      byte[] collation = null;
      int lengthByte = -1;
      byte scale = 0;
      // Precision is often read for Numeric/Decimal types
      byte precision = 0;

      final byte rawType = payload.get();
      final TdsType tdsType = TdsType.valueOf(rawType);

      if (tdsType == null) {
        throw new IllegalStateException("Unknown Type: " + rawType);
      }

      // --- Generic Strategy-Based Parsing ---
      // No more "case 0x26:"

      switch (tdsType.strategy) {
        case FIXED:
          // Fixed types usually don't send metadata length in ColMeta
          // Exception: Some legacy types might, but modern ones don't.
          break;

        case BYTELEN: // e.g., INTN, BITN, DECIMALN
          maxLength = payload.get() & 0xFF;
          if (tdsType == TdsType.DECIMALN || tdsType == TdsType.NUMERICN) {
            precision = payload.get();
            scale = payload.get();
          }
          break;

        case USHORTLEN: // e.g. NVARCHAR, BIGVARCHR
          maxLength = Short.toUnsignedInt(payload.getShort());
          // Collation check
          if (tdsType.r2dbcType == R2dbcType.NVARCHAR || tdsType.r2dbcType == R2dbcType.VARCHAR) {
            collation = new byte[5];
            payload.get(collation);
          }
          break;

        case PLP: // XML
          // Logic for PLP schema...
          break;

        case SCALE_LEN: // Time/Date2
          scale = payload.get();
          break;
      }

      // Column name parsing
      final byte nameLengthInChars = payload.get();
      String columnName = "";
      if (nameLengthInChars > 0) {
        if (nameLengthInChars > 512) throw new IllegalStateException("Suspiciously long column name");
        byte[] nameBytes = new byte[nameLengthInChars * 2];
        payload.get(nameBytes);
        columnName = new String(nameBytes, StandardCharsets.UTF_16LE);
      }

      final ColumnMeta meta = new ColumnMeta(
          colIndex + 1, columnName, (byte)dataType, scale, maxLength, flags, userType, collation, lengthByte
      );
      columns.add(meta);
    }

    final ColMetaDataToken token = new ColMetaDataToken(tokenType, columnCount, columns);
    queryContext.setColMetaDataToken(token);
    return token;
  }
  // Add this helper method to your ColMetaDataTokenParser class
  private void readTableNames(ByteBuffer payload) {
    int numParts = payload.get() & 0xFF; // Number of parts (e.g., 2 for "dbo.AllDataTypes")
    for (int i = 0; i < numParts; i++) {
      int len = payload.getShort() & 0xFFFF; // Length in characters
      byte[] nameBytes = new byte[len * 2];  // 2 bytes per char (Unicode)
      payload.get(nameBytes);
    }
  }
}