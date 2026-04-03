package org.tdslib.javatdslib.tokens.parsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;
import org.tdslib.javatdslib.tokens.models.TypeInfo;
import org.tdslib.javatdslib.transport.ConnectionContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.tdslib.javatdslib.tokens.TokenType.COL_METADATA;

/**
 * Parser for the COLMETADATA token (0x81). This token describes the result set metadata, including
 * column names, types, and lengths.
 */
public class ColMetaDataTokenParser implements TokenParser {
  private static Logger log = LoggerFactory.getLogger(ColMetaDataTokenParser.class);

  @Override
  public Token parse(
      final ByteBuffer payload, final byte tokenType, final ConnectionContext context) {
    if (tokenType != COL_METADATA.getValue()) {
      throw new IllegalArgumentException(
          "Expected COL_METADATA token (0x81), but got 0x"
              + Integer.toHexString(tokenType & 0xFF));
    }

    final short columnCount = payload.getShort();
    final List<ColumnMeta> columns = new ArrayList<>(columnCount);

    for (int colIndex = 0; colIndex < columnCount; colIndex++) {
      final int userType = payload.getInt();
      final short flags = payload.getShort();

      // Delegate Type Parsing
      TypeInfo typeInfo = TypeInfoParser.parse(payload);

      log.trace("colIndex: {} type: {}", colIndex, typeInfo.getTdsType());

      // Column name parsing
      final byte nameLengthInChars = payload.get();
      String columnName = "";
      if (nameLengthInChars > 0) {
        byte[] nameBytes = new byte[nameLengthInChars * 2];
        payload.get(nameBytes);
        columnName = new String(nameBytes, StandardCharsets.UTF_16LE);
      }

      final ColumnMeta meta = new ColumnMeta(colIndex + 1, columnName, userType, flags, typeInfo);
      columns.add(meta);
    }

    return new ColMetaDataToken(tokenType, columnCount, columns);
  }

  @Override
  public boolean canParse(ByteBuffer peekBuffer, ConnectionContext context) {
    // 1. Check for columnCount (2 bytes)
    if (peekBuffer.remaining() < 2) return false;
    short columnCount = peekBuffer.getShort();

    if (columnCount == (short) 0xFFFF) {
      return true; // 0xFFFF means no metadata
    }

    for (int i = 0; i < columnCount; i++) {
      // 2. Check for userType (4 bytes) and flags (2 bytes) = 6 total bytes
      if (peekBuffer.remaining() < 6) return false;
      peekBuffer.getInt();   // userType
      peekBuffer.getShort(); // flags

      // 3. Safely delegate to TypeInfoParser to check if it can parse
      if (!TypeInfoParser.canParse(peekBuffer)) {
        return false;
      }

      // 4. Check for nameLengthInChars (1 byte)
      if (peekBuffer.remaining() < 1) return false;
      byte nameLengthInChars = peekBuffer.get();
      int nameBytes = nameLengthInChars * 2;

      // 5. Check for the string payload
      if (peekBuffer.remaining() < nameBytes) return false;
      peekBuffer.position(peekBuffer.position() + nameBytes);
    }

    return true;
  }
}