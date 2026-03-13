package org.tdslib.javatdslib.tokens;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.TdsType.LengthStrategy;
import org.tdslib.javatdslib.tokens.models.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.models.ColumnMeta;

/**
 * Utility to proactively calculate the byte length of a ROW token without materializing data.
 */
public class RowBoundaryScanner {
  private static final Logger logger = LoggerFactory.getLogger(RowBoundaryScanner.class);

  /**
   * Calculates the exact byte length of the current ROW token.
   * Throws BufferUnderflowException if the buffer does not contain the full row.
   *
   * @param buffer The network buffer, positioned at the start of the row data.
   * @param metadata The current result set's metadata blueprint.
   * @return The exact number of payload bytes this row consumes.
   */
  public static int calculateRowLength(ByteBuffer buffer, ColMetaDataToken metadata) {
    if (metadata == null) {
      throw new IllegalStateException("Cannot scan ROW boundaries without ColMetaDataToken");
    }

    int startPosition = buffer.position();

    try {
      for (ColumnMeta col : metadata.getColumns()) {
        TdsType tdsType = col.getTypeInfo().getTdsType();
        LengthStrategy strategy = tdsType.strategy;

        if (strategy == LengthStrategy.FIXED) {
          skip(buffer, tdsType.fixedSize);

        } else if (strategy == LengthStrategy.BYTELEN || strategy == LengthStrategy.SCALE_LEN || strategy == LengthStrategy.PREC_SCALE) {
          int len = Byte.toUnsignedInt(buffer.get());
          if (len != 0 && len != 0xFF) { // 0xFF (255) is often the NULL indicator
            skip(buffer, len);
          }

        } else if (strategy == LengthStrategy.USHORTLEN) {
          int len = Short.toUnsignedInt(buffer.getShort());
          if (len != 0xFFFF) { // 0xFFFF is the NULL indicator
            skip(buffer, len);
          }

        } else if (strategy == LengthStrategy.LONGLEN) {
          int len = buffer.getInt();
          if (len != -1) { // -1 is the NULL indicator for LONGLEN
            // Note: TEXT/IMAGE types have complex headers (textptr).
            // Expand this logic if legacy TEXT types are strictly required.
            skip(buffer, len);
          }

        } else if (strategy == LengthStrategy.PLP) {
          long totalLen = buffer.getLong(); // Total length or 0xFFFFFFFFFFFFFFFF (unknown)
          while (true) {
            int chunkLen = buffer.getInt();
            if (chunkLen == 0) {
              break; // PLP Terminator
            }
            skip(buffer, chunkLen);
          }
        }
      }

      int rowLength = buffer.position() - startPosition;
      buffer.position(startPosition); // Rewind so the actual parser can consume it
      return rowLength;

    } catch (BufferUnderflowException e) {
      buffer.position(startPosition); // Rewind before bubbling up the exception
      throw e;
    }
  }

  private static void skip(ByteBuffer buffer, int bytesToSkip) {
    if (buffer.remaining() < bytesToSkip) {
      throw new BufferUnderflowException();
    }
    buffer.position(buffer.position() + bytesToSkip);
  }
}