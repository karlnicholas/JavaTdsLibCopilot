package org.tdslib.javatdslib;

/**
 * Registry of TDS Data Type constants (XTYPE/Token values).
 * Includes both Fixed-Length (Legacy) and Variable-Length/Nullable (N-Type) versions.
 */
public final class TdsDataType {
  private TdsDataType() {}

  // --- Fixed-Length Integer Types ---
  public static final int INT1 = 0x30;        // TinyInt
  public static final int INT2 = 0x34;        // SmallInt
  public static final int INT4 = 0x38;        // Int
  public static final int INT8 = 0x7F;        // BigInt

  // --- Nullable/Variable Integer Types (RPC & Modern Results) ---
  public static final int INTN = 0x26;        // Nullable Integer (covers Tiny, Small, Int, Big)

  // --- Bit Types ---
  public static final int BIT  = 0x32;        // Fixed Bit
  public static final int BITN = 0x68;        // Nullable Bit

  // --- Floating Point Types ---
  public static final int FLT4 = 0x3B;        // Real (4 byte)
  public static final int FLT8 = 0x3E;        // Float (8 byte)
  public static final int FLTN = 0x6D;        // Nullable Float/Real

  // --- Decimal / Numeric ---
  public static final int NUMERIC  = 0x3F;    // Fixed Numeric
  public static final int DECIMAL  = 0x37;    // Fixed Decimal
  public static final int NUMERICN = 0x6C;    // Nullable Numeric
  public static final int DECIMALN = 0x6A;    // Nullable Decimal

  // --- Money ---
  public static final int MONEY  = 0x3C;      // Fixed Money (8 byte)
  public static final int MONEY4 = 0x7A;      // SmallMoney (4 byte)
  public static final int MONEYN = 0x6E;      // Nullable Money

  // --- Date / Time ---
  public static final int DATETIME   = 0x3D;  // Fixed DateTime (8 byte)
  public static final int DATETIM4   = 0x3A;  // SmallDateTime (4 byte)
  public static final int DATETIMN   = 0x6F;  // Nullable DateTime
  public static final int DATE       = 0x28;  // Date (3 byte)
  public static final int TIME       = 0x29;  // Time (variable scale)
  public static final int DATETIME2  = 0x2A;  // DateTime2 (variable scale)
  public static final int DATETIMEOFFSET = 0x2B; // DateTimeOffset

  // --- Strings / Characters ---
  public static final int CHAR      = 0x2F;   // Fixed Char
  public static final int VARCHAR   = 0x27;   // Variable Char (Legacy)
  public static final int TEXT      = 0x23;   // Text
  public static final int NCHAR     = 0xEF;   // Fixed NChar
  public static final int NVARCHAR  = 0xE7;   // Variable NChar (Legacy & Modern)
  public static final int NTEXT     = 0x63;   // NText
  public static final int BIGVARCHR = 0xA7;   // Large Variable Char (VARCHAR(MAX))
  public static final int BIGCHAR   = 0xAF;   // Large Fixed Char (Modern CHAR)

  // --- Binary ---
  public static final int BINARY    = 0x2D;   // Fixed Binary
  public static final int VARBINARY = 0x25;   // Variable Binary (Legacy)
  public static final int IMAGE     = 0x22;   // Image
  public static final int BIGVARBIN = 0xA5;   // Large Variable Binary (VARBINARY(MAX))
  public static final int BIGBINARY = 0xAD;   // Large Fixed Binary (Binary(n))

  // --- Specialized ---
  public static final int GUID      = 0x24;   // UniqueIdentifier
  public static final int XML       = 0xF1;   // XML
  public static final int UDT       = 0xF0;   // CLR UDT (Geography/Geometry)
  public static final int SSVARIANT = 0x62;   // SQL_VARIANT
}