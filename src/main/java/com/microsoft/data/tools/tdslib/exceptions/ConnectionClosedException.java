package com.microsoft.data.tools.tdslib.exceptions;

/**
 * Thrown when a connection is closed unexpectedly.
 */
public class ConnectionClosedException extends RuntimeException {
    public ConnectionClosedException() {
        super();
    }

    public ConnectionClosedException(String message) {
        super(message);
    }

    public ConnectionClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionClosedException(Throwable cause) {
        super(cause);
    }
}
