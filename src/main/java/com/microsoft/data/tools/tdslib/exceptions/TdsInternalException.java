package com.microsoft.data.tools.tdslib.exceptions;

/**
 * Exception for TDS protocol internal errors.
 */
public class TdsInternalException extends RuntimeException {
    public TdsInternalException() {
        super();
    }

    public TdsInternalException(String message) {
        super(message);
    }

    public TdsInternalException(String message, Throwable cause) {
        super(message, cause);
    }

    public TdsInternalException(Throwable cause) {
        super(cause);
    }
}
