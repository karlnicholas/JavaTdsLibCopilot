package com.microsoft.data.tools.tdslib.exceptions;

import java.io.IOException;

/**
 * Exception for TDS protocol IO errors.
 */
public class TdsIOException extends IOException {
    public TdsIOException() {
        super();
    }

    public TdsIOException(String message) {
        super(message);
    }

    public TdsIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public TdsIOException(Throwable cause) {
        super(cause);
    }
}
