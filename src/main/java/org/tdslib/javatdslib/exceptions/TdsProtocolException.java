package org.tdslib.javatdslib.exceptions;

/**
 * Exception for TDS protocol violations.
 */
public class TdsProtocolException extends TdsException {
    public TdsProtocolException() {
        super();
    }

    public TdsProtocolException(String message) {
        super(message);
    }

    public TdsProtocolException(String message, Throwable cause) {
        super(message, cause);
    }

    public TdsProtocolException(Throwable cause) {
        super(cause);
    }
}
