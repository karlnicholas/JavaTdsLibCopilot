package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.tokens.models.TypeInfo;

import java.nio.ByteBuffer;

/** Shared parser for the TYPE_INFO stream structure. */
public class TypeInfoParser {

  /**
   * Parses the TYPE_INFO structure from the payload.
   *
   * @param payload The buffer containing the TYPE_INFO data.
   * @return The parsed TypeInfo object.
   */
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
          throw new UnsupportedOperationException(
              "XML with Schema validation is not yet supported");
        }
        break;

      case SCALE_LEN:
        scale = payload.get();
        break;

      default:
        throw new IllegalArgumentException("Unsupported length strategy: " + tdsType.strategy);
    }

    return new TypeInfo(tdsType, maxLength, precision, scale, collation);
  }

  // Replace this helper method at the bottom of the class
  private static boolean isTextType(TdsType type) {
    return type == TdsType.NVARCHAR
        || type == TdsType.BIGVARCHR
        || type == TdsType.VARCHAR
        || type == TdsType.NCHAR
        || type == TdsType.BIGCHAR
        || type == TdsType.CHAR;
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
  /**
   * Safely calculates the required bytes for the TYPE_INFO structure and advances
   * the peekBuffer position if enough bytes are available.
   *
   * @param peekBuffer A read-only duplicate of the current network buffer.
   * @return The number of bytes required, or -1 if the buffer underflows.
   */
  public static int getRequiredBytes(ByteBuffer peekBuffer) {
    int startPos = peekBuffer.position();

    // 1. Check Data Type
    if (peekBuffer.remaining() < 1) return -1;
    int dataTypeByte = peekBuffer.get() & 0xFF;
    TdsType tdsType = TdsType.valueOf((byte) dataTypeByte);

    if (tdsType == null) {
      throw new IllegalStateException("Unknown Type: 0x" + Integer.toHexString(dataTypeByte));
    }

    // 2. Check details based on Length Strategy
    switch (tdsType.strategy) {
      case FIXED:
        break; // No extra bytes needed

      case BYTELEN:
        if (tdsType == TdsType.DECIMALN || tdsType == TdsType.NUMERICN) {
          if (peekBuffer.remaining() < 3) return -1;
          peekBuffer.position(peekBuffer.position() + 3); // maxLength, precision, scale
        } else {
          if (peekBuffer.remaining() < 1) return -1;
          peekBuffer.position(peekBuffer.position() + 1); // maxLength
        }
        break;

      case PREC_SCALE:
        if (peekBuffer.remaining() < 3) return -1;
        peekBuffer.position(peekBuffer.position() + 3); // maxLength, precision, scale
        break;

      case USHORTLEN:
        int bytesRequiredUshort = 2; // maxLength
        if (isTextType(tdsType)) {
          bytesRequiredUshort += 5; // collation
        }
        if (peekBuffer.remaining() < bytesRequiredUshort) return -1;
        peekBuffer.position(peekBuffer.position() + bytesRequiredUshort);
        break;

      case LONGLEN:
        int bytesRequiredLong = 4; // maxLength
        if (tdsType == TdsType.TEXT || tdsType == TdsType.NTEXT) {
          bytesRequiredLong += 5; // collation
        }
        if (peekBuffer.remaining() < bytesRequiredLong) return -1;
        peekBuffer.position(peekBuffer.position() + bytesRequiredLong);

        // Consume Table Names safely
        if (!skipTableNames(peekBuffer)) {
          return -1;
        }
        break;

      case PLP:
        if (peekBuffer.remaining() < 1) return -1;
        peekBuffer.position(peekBuffer.position() + 1); // schemaPresent
        break;

      case SCALE_LEN:
        if (peekBuffer.remaining() < 1) return -1;
        peekBuffer.position(peekBuffer.position() + 1); // scale
        break;

      default:
        throw new IllegalArgumentException("Unsupported length strategy: " + tdsType.strategy);
    }

    return peekBuffer.position() - startPos;
  }

  /**
   * Safely skips table names without throwing BufferUnderflowExceptions.
   */
  private static boolean skipTableNames(ByteBuffer peekBuffer) {
    if (peekBuffer.remaining() < 1) return false;
    int numParts = peekBuffer.get() & 0xFF;

    for (int i = 0; i < numParts; i++) {
      if (peekBuffer.remaining() < 2) return false;
      int len = peekBuffer.getShort() & 0xFFFF; // Length in chars
      int byteLen = len * 2;

      if (peekBuffer.remaining() < byteLen) return false;
      peekBuffer.position(peekBuffer.position() + byteLen);
    }
    return true;
  }
}
