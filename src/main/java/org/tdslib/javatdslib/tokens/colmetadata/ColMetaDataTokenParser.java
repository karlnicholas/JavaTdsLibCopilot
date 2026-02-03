package org.tdslib.javatdslib.tokens.colmetadata;

import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsDataType;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ColMetaDataTokenParser implements TokenParser {

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

      int maxLength = -1;
      byte[] collation = null;
      int lengthByte = -1;
      byte scale = 0;
      // Precision is often read for Numeric/Decimal types
      byte precision = 0;

      switch (dataType) {
        // --- 1. Variable-Length with Collation (Strings) ---
        case TdsDataType.BIGVARCHR: // 0xA7
        case TdsDataType.NVARCHAR:  // 0xE7
        case TdsDataType.VARCHAR:   // 0x27 (Legacy)
        case TdsDataType.NCHAR:     // 0xEF
        case TdsDataType.CHAR:      // 0x2F
        case TdsDataType.TEXT:      // 0x23
        case TdsDataType.NTEXT:     // 0x63
          maxLength = payload.getShort() & 0xFFFF;
          collation = new byte[5];
          payload.get(collation);
          break;

        // --- 2. Variable-Length without Collation (Binary/Image/GUID) ---
        case TdsDataType.BIGVARBIN: // 0xA5
        case TdsDataType.VARBINARY: // 0x25
        case TdsDataType.BINARY:    // 0x2D
        case TdsDataType.IMAGE:     // 0x22
        case TdsDataType.UDT:       // 0xF0 (Geography/Geometry)
        case TdsDataType.XML:       // 0xF1
        case TdsDataType.SSVARIANT: // 0x62
          maxLength = payload.getShort() & 0xFFFF;
          break;

        // --- 3. Fixed-Length Types ---
        case TdsDataType.INT8:      // 0x7F
        case TdsDataType.DATETIME:  // 0x3D
        case TdsDataType.FLT8:      // 0x3E
        case TdsDataType.MONEY:     // 0x3C
          maxLength = 8;
          break;

        case TdsDataType.INT4:      // 0x38
        case TdsDataType.FLT4:      // 0x3B
        case TdsDataType.MONEY4:    // 0x7A
        case TdsDataType.DATETIM4:  // 0x3A
        case TdsDataType.INT2:      // 0x34
          maxLength = 4; // Note: INT2 is 2, DATETIM4 is 4, etc.
          if (dataType == TdsDataType.INT2) maxLength = 2;
          break;

        case TdsDataType.INT1:      // 0x30
        case TdsDataType.BIT:       // 0x32
          maxLength = 1;
          break;

        case TdsDataType.DATE:      // 0x28
          maxLength = 3;
          break;

        case TdsDataType.GUID:      // 0x24
          // GUID can be treated as fixed 16 in some contexts or varlen in others depending on version
          // usually in COLMETADATA it's explicit length or fixed 16.
          // 0x24 in COLMETADATA often has a 1-byte length after it (0x10).
          // Let's assume standard behavior similar to INTN for now or fix length
          lengthByte = payload.get();
          maxLength = lengthByte;
          break;

        // --- 4. Length-Prefixed (Nullable) Types ---
        case TdsDataType.INTN:      // 0x26
        case TdsDataType.BITN:      // 0x68
        case TdsDataType.FLTN:      // 0x6D
        case TdsDataType.DATETIMN:  // 0x6F
        case TdsDataType.MONEYN:    // 0x6E
          lengthByte = payload.get();
          maxLength = lengthByte;
          break;

        // --- 5. Precision/Scale Types ---
        case TdsDataType.NUMERICN:  // 0x6C
        case TdsDataType.DECIMALN:  // 0x6A
          lengthByte = payload.get(); // Max Len
          maxLength = lengthByte;
          precision = payload.get();
          scale = payload.get();
          break;

        case TdsDataType.DATETIME2:       // 0x2A
        case TdsDataType.DATETIMEOFFSET:  // 0x2B
        case TdsDataType.TIME:            // 0x29
          scale = payload.get(); // Scale (0-7)
          // length implies max bytes for that scale (e.g. 7 bytes for datetime2(7))
          break;

        default:
          throw new IllegalStateException("Unknown DataType in ColMetaData: 0x" + Integer.toHexString(dataType));
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
}