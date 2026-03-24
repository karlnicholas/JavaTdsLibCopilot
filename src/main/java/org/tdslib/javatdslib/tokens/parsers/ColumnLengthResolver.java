package org.tdslib.javatdslib.tokens.parsers;

import org.tdslib.javatdslib.protocol.TdsType;

import java.nio.ByteBuffer;

public class ColumnLengthResolver {

  // NEW: Signal that the network cut off the length header itself
  public static final int INCOMPLETE_HEADER = -2;

  /**
   * Calculates the expected byte length of a standard column based on its TdsType.
   * Does NOT handle PLP chunks (strategy == PLP).
   * @return The length in bytes to read, -1 if the value is SQL NULL, or INCOMPLETE_HEADER (-2)
   */
  public static int resolveStandardLength(ByteBuffer payload, TdsType type, int maxLength) {
    switch (type.strategy) {
      case FIXED:
        if (type == TdsType.DATE) {
          if (payload.remaining() < 1) return INCOMPLETE_HEADER;
          int len = payload.get() & 0xFF;
          return len == 0 ? -1 : len;
        }
        return type.fixedSize;

      case SCALE_LEN:
      case PREC_SCALE:
      case BYTELEN:
        if (payload.remaining() < 1) return INCOMPLETE_HEADER;
        int bLen = payload.get() & 0xFF;
        return bLen == 0 ? -1 : bLen;

      case USHORTLEN:
        // Note: 65535 (0xFFFF) typically signals PLP or NULL depending on the context.
        // Assuming standard VarLen for this branch.
        if (payload.remaining() < 2) return INCOMPLETE_HEADER;
        int varLen = Short.toUnsignedInt(payload.getShort());
        return varLen == 0xFFFF ? -1 : varLen;

      case LONGLEN:
        // TEXT/IMAGE/NTEXT headers
        if (payload.remaining() < 1) return INCOMPLETE_HEADER;
        int textPtrLen = payload.get() & 0xFF;

        if (textPtrLen == 0) {
          return -1; // Null
        }

        // We need (textPtrLen) + 8 (timestamp) + 4 (data length) bytes to determine the actual data length
        int requiredHeaderBytes = textPtrLen + 8 + 4;
        if (payload.remaining() < requiredHeaderBytes) {
          return INCOMPLETE_HEADER;
        }

        // Skip text pointer and timestamp
        payload.position(payload.position() + textPtrLen + 8);

        // Read the actual data length
        return payload.getInt();

      default:
        throw new IllegalArgumentException("Unsupported or PLP length strategy in standard parser: " + type.strategy);
    }
  }
}