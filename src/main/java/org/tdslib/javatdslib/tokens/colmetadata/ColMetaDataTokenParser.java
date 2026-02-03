package org.tdslib.javatdslib.tokens.colmetadata;

import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for COLMETADATA token (0x81) – column metadata.
 * Fully aligned with TDS 7.2+ (SQL Server 2005+) structure.
 * Tested against Wireshark decode showing two NVARCHAR columns:
 * "version" and "db".
 */
public class ColMetaDataTokenParser implements TokenParser {

  @Override
  public Token parse(final ByteBuffer payload, final byte tokenType,
                     final ConnectionContext context, final QueryContext queryContext) {
    if (tokenType != (byte) 0x81) {
      throw new IllegalArgumentException(
          "Expected COL_METADATA token (0x81), but got 0x"
              + Integer.toHexString(tokenType & 0xFF));
    }

    // Number of columns (USHORT)
    final short columnCount = payload.getShort();

    final List<ColumnMeta> columns = new ArrayList<>(columnCount);

    for (int colIndex = 0; colIndex < columnCount; colIndex++) {
      final int userType = payload.getInt();      // ULONG
      final short flags    = payload.getShort();  // USHORT
      final byte dataType  = payload.get();       // BYTE

      int maxLength = -1;
      byte[] collation = null;
      int lengthByte = -1;                     // for types like INTNTYPE
      byte scale = 0;

      switch (dataType) {
        // 1. Variable-length character types: 2-byte MaxLen + 5-byte Collation
        case (byte) 0x27:   // VARCHAR
        case (byte) 0xE7:   // NVARCHAR
        case (byte) 0x2F:   // CHAR
        case (byte) 0xEF:   // NCHAR
        case (byte) 0xA7:   // BIGVARCHRTYPE (Type 167)
          maxLength = payload.getShort() & 0xFFFF;
          collation = new byte[5];
          payload.get(collation);
          break;

        // 2. Fixed-length types: No extra TypeInfo bytes to read
        case (byte) 0x7F:   // BIGINT (0x7F = 127) -> 8 bytes
          maxLength = 8;
          break;
        case (byte) 0x38:   // INT -> 4 bytes
          maxLength = 4;
          break;
        case (byte) 0x28:   // DATE -> 3 bytes
          maxLength = 3;
          break;
        case (byte) 0x32:   // BIT -> 1 byte
          maxLength = 1;
          break;

        // 3. Variable-length Numeric/DateTime types: 1-byte length prefix
        case (byte) 0x26:   // INTNTYPE (Nullable TinyInt, SmallInt, Int, BigInt)
        case (byte) 0x6D:   // FLOATNTYPE (Nullable Real, Float)
        case (byte) 0x6F:   // DATETIMENTYPE (Nullable DateTime)
          lengthByte = payload.get();
          maxLength = lengthByte;
          break;

        case (byte) 0x2A:   // DATETIME2(n)
          scale = payload.get(); // 0-7 fractional seconds precision
          break;

        default:
          // Log or handle unknown type 0x...
          break;
      }

      // Column name (common to all)
      final byte nameLengthInChars = payload.get();  // BYTE count of UTF-16LE chars
      String columnName = "";

      if (nameLengthInChars > 0) {
        if (nameLengthInChars > 512) {
          throw new IllegalStateException("Suspiciously long column name: " + nameLengthInChars);
        }
        byte[] nameBytes = new byte[nameLengthInChars * 2];
        payload.get(nameBytes);
        columnName = new String(nameBytes, StandardCharsets.UTF_16LE);
      }

      // Build metadata – you can add more fields to ColumnMeta if useful
      // e.g. lengthByte, actual maxLength interpretation, etc.
      final ColumnMeta meta = new ColumnMeta(
          colIndex + 1,
          columnName,
          dataType,
          scale,
          maxLength,
          flags,
          userType,
          collation,
          lengthByte   // optional: store for int types
      );

      columns.add(meta);
    }

    final ColMetaDataToken token = new ColMetaDataToken(tokenType, columnCount,
        columns);
    queryContext.setColMetaDataToken(token);
    return token;
  }

  /**
   * Returns true for types that include max length + collation (e.g.
   * NVARCHAR, VARCHAR, NCHAR, CHAR).
   */
  private boolean isVariableLengthStringType(final byte dataType) {
    return dataType == (byte) 0xE7 // NVARCHAR
        || dataType == (byte) 0xEF // NCHAR
        || dataType == (byte) 0x27 // VARCHAR
        || dataType == (byte) 0x2F; // CHAR
  }

}
