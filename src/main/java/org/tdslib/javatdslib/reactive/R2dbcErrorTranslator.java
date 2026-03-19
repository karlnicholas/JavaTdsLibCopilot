package org.tdslib.javatdslib.reactive;

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.tdslib.javatdslib.protocol.TdsServerErrorException;

/**
 * Translator for converting TDS protocol errors into R2DBC exceptions.
 */
public class R2dbcErrorTranslator {

  /**
   * Translates an internal TDS error into the appropriate R2DBC SPI exception.
   */
  public static R2dbcException translateException(TdsServerErrorException tdsError) {
    long errorCode = tdsError.getErrorNumber();
    String msg = tdsError.getMessage();
    String sqlState = mapToSqlState(errorCode);

    return switch ((int) errorCode) {
      case 208, 2812 -> new R2dbcBadGrammarException(msg, sqlState, (int) errorCode);
      case 547, 2601, 2627 -> new R2dbcDataIntegrityViolationException(msg, sqlState, (int) errorCode);
      case 229, 230 -> new R2dbcPermissionDeniedException(msg, sqlState, (int) errorCode);
      case 1205 -> new R2dbcTransientResourceException(msg, sqlState, (int) errorCode);
      default -> {
        if (tdsError.getSeverity() >= 20) {
          yield new R2dbcNonTransientResourceException(msg, sqlState, (int) errorCode);
        }
        yield new R2dbcException(msg, sqlState, (int) errorCode) {};
      }
    };
  }

  private static String mapToSqlState(long errorCode) {
    // Basic mapping for example purposes
    return switch ((int) errorCode) {
      case 208 -> "42S02"; // Invalid object name
      case 547 -> "23000"; // Constraint violation
      case 2627, 2601 -> "23000"; // Unique violation
      default -> "HY000"; // Generic error
    };
  }
}
