package org.tdslib.javatdslib.protocol;

/**
 * A neutral, internal representation of a SQL Parameter.
 * Decoupled from R2DBC SPI.
 */
public record TdsParameter(
    TdsType type,
    String name,
    Object value,
    boolean isOutParameter
) {}