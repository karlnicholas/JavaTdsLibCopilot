package org.tdslib.javatdslib.exceptions;

/**
 * Exception for unsupported TDS features.
 */
public class TdsUnsupportedFeatureException extends TdsException {
    public TdsUnsupportedFeatureException() {
        super();
    }

    public TdsUnsupportedFeatureException(String message) {
        super(message);
    }

    public TdsUnsupportedFeatureException(String message, Throwable cause) {
        super(message, cause);
    }

    public TdsUnsupportedFeatureException(Throwable cause) {
        super(cause);
    }
}
