package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.R2dbcType;
import org.tdslib.javatdslib.protocol.TdsType;

/**
 * Maps standard R2DBC SPI Types to and from internal MS-TDS Protocol Types.
 */
public class R2dbcTypeMapper {

  /**
   * Translates an R2DBC type to the appropriate TDS parameter type for RPC calls.
   */
  public static TdsType toTdsType(R2dbcType type) {
    return switch (type) {
      case TINYINT, SMALLINT, INTEGER, BIGINT -> TdsType.INTN;
      case REAL -> TdsType.REAL;
      case DOUBLE, FLOAT -> TdsType.FLTN;
      case DECIMAL -> TdsType.DECIMALN;
      case NUMERIC -> TdsType.NUMERICN;
      case BOOLEAN -> TdsType.BITN;
      case DATE -> TdsType.DATE;
      case TIME -> TdsType.TIME;
      case TIMESTAMP -> TdsType.DATETIME2;
      case TIMESTAMP_WITH_TIME_ZONE -> TdsType.DATETIMEOFFSET;
      case VARCHAR -> TdsType.BIGVARCHR;
      case NVARCHAR -> TdsType.NVARCHAR;
      case CHAR -> TdsType.BIGCHAR;
      case NCHAR -> TdsType.NCHAR;
      case BINARY -> TdsType.BIGBINARY;
      case VARBINARY -> TdsType.BIGVARBIN;
      case CLOB -> TdsType.TEXT;
      case BLOB -> TdsType.IMAGE;
      default -> TdsType.NVARCHAR; // Safe fallback
    };
  }

  /**
   * Translates an internal TDS type to an R2DBC type (useful for Column Metadata).
   */
  public static R2dbcType toR2dbcType(TdsType type) {
    return switch (type) {
      case INT1 -> R2dbcType.TINYINT;
      case INT2 -> R2dbcType.SMALLINT;
      case INT4, INTN -> R2dbcType.INTEGER;
      case INT8 -> R2dbcType.BIGINT;
      case BIT, BITN -> R2dbcType.BOOLEAN;
      case FLT4, REAL -> R2dbcType.REAL;
      case FLT8, FLTN -> R2dbcType.DOUBLE;
      case DECIMAL, DECIMALN, MONEY, MONEYN, SMALLMONEY -> R2dbcType.DECIMAL;
      case NUMERIC, NUMERICN -> R2dbcType.NUMERIC;
      case DATE -> R2dbcType.DATE;
      case TIME -> R2dbcType.TIME;
      case DATETIME2, DATETIME, DATETIMN, SMALLDATETIME -> R2dbcType.TIMESTAMP;
      case DATETIMEOFFSET -> R2dbcType.TIMESTAMP_WITH_TIME_ZONE;
      case BIGVARCHR, VARCHAR, TEXT -> R2dbcType.VARCHAR;
      case NVARCHAR, NTEXT, XML -> R2dbcType.NVARCHAR;
      case BIGCHAR, CHAR, GUID -> R2dbcType.CHAR;
      case NCHAR -> R2dbcType.NCHAR;
      case BIGBINARY, BINARY -> R2dbcType.BINARY;
      case BIGVARBIN, VARBINARY, IMAGE -> R2dbcType.VARBINARY;
    };
  }
}