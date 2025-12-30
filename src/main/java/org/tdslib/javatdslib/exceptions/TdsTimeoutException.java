package org.tdslib.javatdslib.exceptions;

import java.io.InterruptedIOException;

/**
 * Exception for TDS protocol timeout errors.
 */
public class TdsTimeoutException extends InterruptedIOException {
    public TdsTimeoutException() {
        super();
    }

    public TdsTimeoutException(String message) {
        super(message);
    }

    public TdsTimeoutException(String message, Throwable cause) {
        super(message);
        initCause(cause);
    }

    public TdsTimeoutException(Throwable cause) {
        super();
        initCause(cause);
    }
}
