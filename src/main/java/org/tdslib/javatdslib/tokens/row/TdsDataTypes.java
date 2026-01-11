package org.tdslib.javatdslib.tokens.row;

/**
 * Holds constants and helper methods for TDS data types to avoid hard-coding
 * in parsers. This class can be expanded as more types are supported.
 *
 * <p>Note: This focuses on common types for result sets. Nullable types (\*N)
 * use BYTE length prefix; fixed non-nullable use direct data; variables use
 * USHORT. Corrected based on TDS specification: INT (4 bytes) is 0x38,
 * TINYINT (1 byte) is 0x30, etc.
 */
public class TdsDataTypes {

  // Common data type codes (hex values from TDS spec)
  public static final byte INTNTYPE = 0x26; // Nullable int (BYTE len: 0,1,2,4,8)
  public static final byte TINYINTTYPE = 0x30; // Tinyint (1 byte, fixed) - SYBINT1
  public static final byte SMALLINTTYPE = 0x34; // Smallint (2 bytes, fixed) - SYBINT2
  public static final byte INTTYPE = 0x38; // Int (4 bytes, fixed) - SYBINT4
  public static final byte FLOATNTYPE = 0x6D; // Nullable float (BYTE len: 0,4,8)
  public static final byte REALTYPE = 0x3B; // Real (4 bytes, fixed) - SYBREAL
  public static final byte FLOATTYPE = 0x3E; // Float (8 bytes, fixed) - SYBFLT8
  public static final byte SMALLDATETIMETYPE = 0x3A; // Smalldatetime (4 bytes) - SYBDATETIME4
  public static final byte BIGDATETIMETYPE = 0x3D; // Datetime (8 bytes, fixed) - SYBDATETIME
  public static final byte BIGINTTYPE = 0x7F; // Bigint (8 bytes, fixed) - SYBINT8
  // Add more as needed, e.g., BITTYPE = 0x32 (1 byte, fixed), DATETIMNTYPE = 0x6F, etc.
  // Variable types like VARCHAR (0xA7) use USHORT length.

  /**
   * Returns true if the provided TDS type code corresponds to a fixed-length
   * type.
   *
   * @param type TDS data type code
   * @return true if fixed-length
   */
  public static boolean isFixedLength(byte type) {
    switch (type) {
      case TINYINTTYPE:      // 0x30
      case SMALLINTTYPE:     // 0x34
      case INTTYPE:          // 0x38
      case REALTYPE:         // 0x3B
      case FLOATTYPE:        // 0x3E
      case SMALLDATETIMETYPE:// 0x3A
      case BIGINTTYPE:       // 0x7F
        return true;
      default:
        return false;
    }
  }

  /**
   * Returns the fixed byte length for the given TDS type code, or 0 if the
   * type is not fixed-length.
   *
   * @param type TDS data type code
   * @return length in bytes, or 0 if variable/not fixed
   */
  public static int getFixedLength(byte type) {
    switch (type) {
      case TINYINTTYPE:
        return 1;
      case SMALLINTTYPE:
        return 2;
      case INTTYPE:
        return 4;
      case REALTYPE:
        return 4;
      case FLOATTYPE:
        return 8;
      case SMALLDATETIMETYPE:
        return 4;
      case BIGINTTYPE:
        return 8;
      default:
        return 0;
    }
  }

  /**
   * Returns true if the provided TDS type code is a nullable fixed-length
   * type (i.e., fixed-length when present but can be nullable).
   *
   * @param type TDS data type code
   * @return true if nullable fixed-length
   */
  public static boolean isNullableFixedLength(byte type) {
    switch (type) {
      case INTNTYPE:      // 0x26
      case FLOATNTYPE:    // 0x6D
        // Add others like BITNTYPE 0x68, DATETIMNTYPE 0x6F, etc.
        return true;
      default:
        return false;
    }
  }

  // Could add getExpectedLengthsForNullable(byte type) if needed, e.g., for validation.
}
