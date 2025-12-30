package org.tdslib.javatdslib.exceptions;

/**
 * Exception for TDS login errors.
 */
public class TdsLoginException extends TdsException {
    public TdsLoginException() {
        super();
    }

    public TdsLoginException(String message) {
        super(message);
    }

    public TdsLoginException(String message, Throwable cause) {
        super(message, cause);
    }

    public TdsLoginException(Throwable cause) {
        super(cause);
    }
}
