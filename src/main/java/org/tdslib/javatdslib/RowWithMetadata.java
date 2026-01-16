package org.tdslib.javatdslib;

import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.util.List;

public record RowWithMetadata(List<byte[]> row, List<ColumnMeta> metadata) {}
