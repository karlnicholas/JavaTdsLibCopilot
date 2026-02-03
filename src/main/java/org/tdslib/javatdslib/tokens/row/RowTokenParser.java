package org.tdslib.javatdslib.tokens.row;

import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;
import org.tdslib.javatdslib.transport.ConnectionContext;

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
      throw new IllegalArgumentException("Expected ROW (0xD1), got 0x" +
          Integer.toHexString(tokenType & 0xFF));
    }

    final List<byte[]> columnData = new ArrayList<>();
    final List<ColumnMeta> columns = queryContext.getColMetaDataToken().getColumns();

    if (columns == null || columns.isEmpty()) {
      throw new IllegalStateException("No COLMETADATA available for ROW parsing");
    }

    for (final ColumnMeta col : columns) {
      final byte dataType = col.getDataType();
      final byte[] data;

      switch (dataType) {
        // Fixed-length types: no prefix, known size from type
        case (byte) 0x7F:   // BIGINT (fixed, non-nullable)
        case (byte) 0x38:   // INT
        case (byte) 0x34:   // SMALLINT
        case (byte) 0x30:   // TINYINT
        case (byte) 0x3B:   // REAL (4 bytes)
        case (byte) 0x3E:   // FLOAT (8 bytes)
          int fixedLen = getFixedByteLength(dataType);  // implement below
          byte[] buf = new byte[fixedLen];
          payload.get(buf);
          data = buf;
          break;

        case (byte) 0x28:   // DATE – always exactly 3 bytes
          int dateLen = payload.get() & 0xFF;
          byte[] dateBuf = new byte[dateLen];
          payload.get(dateBuf);
          data = dateBuf;
          break;

        // Nullable INT family (0x26) – 1-byte length prefix
        case (byte) 0x26:
          int len = payload.get() & 0xFF;
          if (len == 0) {
            data = null;
          } else {
            byte[] buf2 = new byte[len];
            payload.get(buf2);
            data = buf2;
          }
          break;

        // DATETIME2(n) – length depends on scale (from metadata)
        case (byte) 0x2A:
          // Assuming you store scale in ColumnMeta (add if missing)
          int scale = col.getScale();  // you need to parse & store this in ColMetaDataTokenParser
          int dt2Len;
          if (scale == 0) dt2Len = 3;          // date only
          else if (scale <= 2) dt2Len = 6;
          else if (scale <= 4) dt2Len = 7;
          else dt2Len = 8;
          int wireLen = payload.get() & 0xFF;
          if (wireLen != 0 && wireLen != dt2Len) {
            throw new IllegalStateException("Unexpected DATETIME2 length: " + wireLen);
          }
          byte[] dt2Buf = new byte[wireLen];
          payload.get(dt2Buf);
          data = dt2Buf;
          break;

        // Variable-length types: USHORT prefix (bytes)
        case (byte) 0xE7:   // NVARCHAR
        case (byte) 0x27:   // VARCHAR
        case (byte) 0xEF:   // NCHAR
        case (byte) 0x2F:   // CHAR
        case (byte) 0xA7:   // BIGVARCHRTYPE (Type 167)
          int varLen = payload.getShort() & 0xFFFF;
          if (varLen == 0xFFFF) {
            data = null;
          } else {
            byte[] varBuf = new byte[varLen];
            payload.get(varBuf);
            data = varBuf;
          }
          break;

        default:
          // Fallback: log warning and skip/safe-read
          // In production: throw or handle gracefully
          System.err.println("Unhandled TDS type in ROW: 0x" + Integer.toHexString(dataType & 0xFF));
          data = null;  // or read minimal to avoid buffer crash
          break;
      }

      columnData.add(data);
    }

    return new RowToken(tokenType, columnData);
  }

  /**
   * Helper: return byte length for fixed types (expand as needed).
   */
  private int getFixedByteLength(byte dataType) {
    return switch (dataType) {
      case (byte) 0x7F -> 8;   // BIGINT
      case (byte) 0x38 -> 4;   // INT
      case (byte) 0x34 -> 2;   // SMALLINT
      case (byte) 0x30 -> 1;   // TINYINT
      case (byte) 0x3B -> 4;   // REAL
      case (byte) 0x3E -> 8;   // FLOAT
      default -> throw new IllegalArgumentException("Unknown fixed length for type 0x" +
          Integer.toHexString(dataType & 0xFF));
    };
  }
}
