package org.tdslib.javatdslib;

import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.util.List;

/**
 * Represents a row together with its column metadata.
 *
 * @param row      the row values as a list of byte arrays
 * @param metadata the list of column metadata for each column in the row
 */
public record RowWithMetadata(List<byte[]> row, List<ColumnMeta> metadata) {}
