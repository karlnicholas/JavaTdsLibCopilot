package org.tdslib.javatdslib.protocol;

import io.r2dbc.spi.*;
import org.tdslib.javatdslib.tokens.models.ErrorToken;

public class SqlErrorTranslator {

  public static R2dbcException createException(ErrorToken error) {
    String message = error.getMessage();
    String sqlState = String.valueOf(error.getState());
    int errorCode = (int) error.getNumber();

    return switch (errorCode) {
      case 102, 156, 170, 208 -> new R2dbcBadGrammarException(message, sqlState, errorCode);
      case 229, 262 -> new R2dbcPermissionDeniedException(message, sqlState, errorCode);
      case 547, 2601, 2627 -> new R2dbcDataIntegrityViolationException(message, sqlState, errorCode);
      case 1205 -> new R2dbcTransientResourceException(message, sqlState, errorCode);
      default -> {
        if (error.getSeverity() >= 19) {
          yield new R2dbcNonTransientResourceException(message, sqlState, errorCode);
        }
        yield new R2dbcNonTransientExceptionSubclass(message, sqlState, errorCode);
      }
    };
  }

  private static class R2dbcNonTransientExceptionSubclass extends io.r2dbc.spi.R2dbcNonTransientException {
    public R2dbcNonTransientExceptionSubclass(String reason, String sqlState, int errorCode) {
      super(reason, sqlState, errorCode);
    }
  }
}