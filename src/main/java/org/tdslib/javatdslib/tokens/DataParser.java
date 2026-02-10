package org.tdslib.javatdslib.tokens;

import org.tdslib.javatdslib.TdsType;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class DataParser {

  public static byte[] getDataBytes(ByteBuffer payload, TdsType type, int maxLength) {
    byte[] data = null;

    switch (type.strategy) {
      case FIXED:
        // DATE (0x28) is special: It is FIXED in Metadata (no length sent)
        // but VARIABLE in RowData (length byte sent: 0x03 or 0x00 for null)
        if (type == TdsType.DATE) {
          int len = payload.get() & 0xFF;
          if (len > 0) {
            data = readBytes(payload, len);
          }
        } else {
          // Standard Fixed types (INT4, FLT8, etc.)
          data = readBytes(payload, type.fixedSize);
        }
        break;

      case SCALE_LEN:  // e.g. DATETIME2, TIME, DATETIMEOFFSET
      case PREC_SCALE: // e.g. Legacy DECIMAL/NUMERIC
      case BYTELEN:    // INTN, BITN, DECIMALN, MONEYN, etc.
        int len = payload.get() & 0xFF;
        if (len > 0) data = readBytes(payload, len);
        break;

      case USHORTLEN: // NVARCHAR, VARBINARY, BIGCHAR, BIGBINARY
        // FIX: Check if this is a MAX type (0xFFFF).
        // If Metadata Length is 65535, the Row Data uses PLP encoding.
        if (maxLength == 65535) {
          data = readPlp(payload);
        } else {
          // Standard short-length string/binary
          int varLen = Short.toUnsignedInt(payload.getShort());
          if (varLen != 0xFFFF) data = readBytes(payload, varLen);
        }
        break;

      case PLP: // XML is always PLP
        data = readPlp(payload);
        break;

      case LONGLEN:
        int textPtrLen = payload.get() & 0xFF;
        if (textPtrLen == 0) {
          data = null;
        } else {
          byte[] txtPtr = new byte[textPtrLen];
          payload.get(txtPtr);
          byte[] timestamp = new byte[8];
          payload.get(timestamp);
          int dataLen = payload.getInt();
          if (dataLen > 0) {
            data = new byte[dataLen];
            payload.get(data);
          } else {
            data = new byte[0];
          }
        }
        break;
    }
    return data;
  }
  private static byte[] readBytes(ByteBuffer buf, int length) {
    byte[] b = new byte[length];
    buf.get(b);
    return b;
  }

  private static byte[] readPlp(ByteBuffer payload) {
    // [MS-TDS] 2.2.5.4.2 Partial Length Prefixed (PLP) DataTypes
    long totalLength = payload.getLong(); // 8 bytes: Total length of data (or -1 if unknown)

    // Note: If totalLength is -1 (0xFFFFFFFFFFFFFFFF), strictly speaking we should still read chunks.
    // But for a simple NULL check in some implementations, 0xFF.. might be used.
    // Usually NULL is represented by the PLP header indicating a null value before this function is called,
    // but in TDS PLP, a null instance is often just a 0xFFFFFFFFFFFFFFFF total length.
    if (totalLength == -1L && payload.remaining() == 0) return null;

    // In many cases, null PLP is handled by the caller or specific header bits,
    // but 0xFFFFFFFFFFFFFFFF is the "PLP Null" marker.
    if (totalLength == 0xFFFFFFFFFFFFFFFFL) return null;

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    // Read Chunks
    while (true) {
      int chunkLen = payload.getInt(); // 4 bytes: Length of current chunk
      if (chunkLen == 0) break;        // PLP_TERMINATOR

      byte[] chunk = new byte[chunkLen];
      payload.get(chunk);
      buffer.writeBytes(chunk);
    }
    return buffer.toByteArray();
  }
}
