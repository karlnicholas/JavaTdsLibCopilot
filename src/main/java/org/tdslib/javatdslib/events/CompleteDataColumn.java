package org.tdslib.javatdslib.events;

/**
 * Represents a fully parsed column payload that fits entirely in memory.
 */
public record CompleteDataColumn(byte[] data) {}

