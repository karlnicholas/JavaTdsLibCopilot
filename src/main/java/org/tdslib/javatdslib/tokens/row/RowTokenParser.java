package org.tdslib.javatdslib.tokens.row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.TdsDataType;
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
      final int dataType = col.getDataType() & 0xFF;
      log.trace("col {} DataType {}", col, dataType);
      byte[] data = null;

      // 1. Check for PLP (Partially Length Prefixed) Types
      // These types (NVARCHAR(MAX), VARBINARY(MAX), XML, UDT) have MaxLength = 0xFFFF in ColMetaData
      boolean isPlp = (dataType == TdsDataType.XML)
              || ((dataType == TdsDataType.NVARCHAR
              || dataType == TdsDataType.BIGVARCHR
              || dataType == TdsDataType.VARBINARY
              || dataType == TdsDataType.BIGVARBIN
              || dataType == TdsDataType.UDT)
              && col.getMaxLength() == 65535);

      if (isPlp) {
        data = readPlp(payload);
      } else {
        switch (dataType) {
          // --- Fixed Length Types (No Length Prefix) ---
          case TdsDataType.INT1:
          case TdsDataType.BIT:
            data = readBytes(payload, 1);
            break;
          case TdsDataType.INT2:
            data = readBytes(payload, 2);
            break;
          case TdsDataType.INT4:
          case TdsDataType.FLT4:
          case TdsDataType.MONEY4:
          case TdsDataType.DATETIM4:
            data = readBytes(payload, 4);
            break;
          case TdsDataType.INT8:
          case TdsDataType.FLT8:
          case TdsDataType.MONEY:
          case TdsDataType.DATETIME:
            data = readBytes(payload, 8);
            break;

          // --- Length-Prefixed (Nullable) Types ---
          // DATE is here because in ROW tokens it has a 1-byte length prefix (0x03)
          case TdsDataType.GUID:
          case TdsDataType.DATE:
          case TdsDataType.INTN:
          case TdsDataType.BITN:
          case TdsDataType.FLTN:
          case TdsDataType.DATETIMN:
          case TdsDataType.MONEYN:
          case TdsDataType.NUMERICN:
          case TdsDataType.DECIMALN:
            int len = payload.get() & 0xFF;
            if (len != 0) {
              data = readBytes(payload, len);
            }
            break;

          // --- Variable Scale Types (1-byte length on wire) ---
          case TdsDataType.DATETIME2:
          case TdsDataType.TIME:
          case TdsDataType.DATETIMEOFFSET:
            int varDtLen = payload.get() & 0xFF;
            if (varDtLen != 0) {
              data = readBytes(payload, varDtLen);
            }
            break;

          // --- Standard Variable Length (Short Prefix) ---
          case TdsDataType.BIGVARCHR:
          case TdsDataType.BIGCHAR:
          case TdsDataType.NVARCHAR:
          case TdsDataType.VARCHAR:
          case TdsDataType.NCHAR:
          case TdsDataType.CHAR:
          case TdsDataType.BIGVARBIN:
          case TdsDataType.BIGBINARY:
          case TdsDataType.VARBINARY:
          case TdsDataType.BINARY:
          case TdsDataType.SSVARIANT:
            int varLen = payload.getShort() & 0xFFFF;
            if (varLen != 0xFFFF) { // 0xFFFF = NULL for standard varlen
              data = readBytes(payload, varLen);
            }
            break;

          case TdsDataType.TEXT:
          case TdsDataType.NTEXT:
          case TdsDataType.IMAGE:
            int textPtrLen = payload.get() & 0xFF; // 1 byte length
            if (textPtrLen != 0) {
              // 1. Read Text Pointer
              readBytes(payload, textPtrLen);

              // 2. Read Timestamp (8 bytes)
              // TODO: What to do with timestamp
              readBytes(payload, 8);

              // 3. Read Data Length (4 bytes)
              int dataLen = payload.getInt();

              // 4. Read Actual Data
              if (dataLen > 0) {
                data = readBytes(payload, dataLen);
              } else {
                data = new byte[0];
              }
            } else {
              // If textPtrLen is 0, the value is NULL
              data = null;
            }
            break;

          default:
            throw new IllegalStateException("Unknown TDS type in ROW: 0x" + Integer.toHexString(dataType));
        }
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