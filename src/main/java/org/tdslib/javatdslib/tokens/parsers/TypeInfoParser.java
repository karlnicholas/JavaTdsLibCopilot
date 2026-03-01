package org.tdslib.javatdslib.tokens.parsers;

import io.r2dbc.spi.R2dbcType;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.tokens.models.TypeInfo;

import java.nio.ByteBuffer;

/**
 * Shared parser for the TYPE_INFO stream structure.
 */
public class TypeInfoParser {

  public static TypeInfo parse(ByteBuffer payload) {
    // 1. Read Data Type
    int dataTypeByte = payload.get() & 0xFF;
    TdsType tdsType = TdsType.valueOf((byte) dataTypeByte);

    if (tdsType == null) {
      throw new IllegalStateException("Unknown Type: 0x" + Integer.toHexString(dataTypeByte));
    }

    int maxLength = -1;
    byte precision = 0;
    byte scale = 0;
    byte[] collation = null;

    // 2. Parse details based on Length Strategy
    switch (tdsType.strategy) {
      case FIXED:
        maxLength = tdsType.fixedSize;
        break;

      case BYTELEN:
        maxLength = payload.get() & 0xFF;
        // Special case for DECIMALN/NUMERICN
        if (tdsType == TdsType.DECIMALN || tdsType == TdsType.NUMERICN) {
          precision = payload.get();
          scale = payload.get();
        }
        break;

      case PREC_SCALE:
        maxLength = payload.get() & 0xFF;
        precision = payload.get();
        scale = payload.get();
        break;

      case USHORTLEN:
        maxLength = Short.toUnsignedInt(payload.getShort());
        if (isTextType(tdsType)) {
          collation = new byte[5];
          payload.get(collation);
        }
        break;

      case LONGLEN:
        maxLength = payload.getInt();
        if (tdsType == TdsType.TEXT || tdsType == TdsType.NTEXT) {
          collation = new byte[5];
          payload.get(collation);
        }
        // Consume Table Names (ignored for now, but must be read to advance stream)
        readTableNames(payload);
        break;

      case PLP:
        byte schemaPresent = payload.get();
        if (schemaPresent != 0) {
          throw new UnsupportedOperationException("XML with Schema validation is not yet supported");
        }
        break;

      case SCALE_LEN:
        scale = payload.get();
        break;
    }

    return new TypeInfo(tdsType, maxLength, precision, scale, collation);
  }

  private static boolean isTextType(TdsType type) {
    return type.r2dbcType == R2dbcType.NVARCHAR ||
        type.r2dbcType == R2dbcType.VARCHAR ||
        type.r2dbcType == R2dbcType.CHAR ||
        type.r2dbcType == R2dbcType.NCHAR;
  }

  private static void readTableNames(ByteBuffer payload) {
    int numParts = payload.get() & 0xFF;
    for (int i = 0; i < numParts; i++) {
      int len = payload.getShort() & 0xFFFF; // Length in chars
      int byteLen = len * 2;
      if (payload.remaining() < byteLen) {
        throw new IllegalStateException("Not enough bytes for table name");
      }
      payload.position(payload.position() + byteLen); // Skip bytes
    }
  }
}