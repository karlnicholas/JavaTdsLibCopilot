package org.tdslib.javatdslib.events;

/**
 * Represents a single chunk of a PLP (Partially Length-Prefixed) stream.
 * * @param chunkData The raw bytes for this specific chunk.
 * @param isLast    True if this is the final chunk (the terminator has been reached).
 */
public record PartialDataColumn(byte[] chunkData, boolean isLast) {}
