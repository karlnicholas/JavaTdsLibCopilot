package org.tdslib.javatdslib.exceptions;

/**
 * Exception for TDS token parsing errors.
 */
public class TdsTokenException extends TdsException {
    public TdsTokenException() {
        super();
    }

    public TdsTokenException(String message) {
        super(message);
    }

    public TdsTokenException(String message, Throwable cause) {
        super(message, cause);
    }

    public TdsTokenException(Throwable cause) {
        super(cause);
    }
}
