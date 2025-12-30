package com.microsoft.data.tools.tdslib.exceptions;

/**
 * Base exception for TDS protocol errors.
 */
public class TdsException extends Exception {
    public TdsException() {
        super();
    }

    public TdsException(String message) {
        super(message);
    }

    public TdsException(String message, Throwable cause) {
        super(message, cause);
    }

    public TdsException(Throwable cause) {
        super(cause);
    }
}
