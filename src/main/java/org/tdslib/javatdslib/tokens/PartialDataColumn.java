package org.tdslib.javatdslib.tokens;

/**
 * Represents a partially fetched data column (e.g. for streaming large payloads).
 */
public class PartialDataColumn implements ColumnData {
  private final int columnIndex;
  private final byte[] chunk;

  /**
   * Constructs a new PartialDataColumn.
   *
   * @param columnIndex The index of the column.
   * @param chunk       The chunk of data.
   */
  public PartialDataColumn(int columnIndex, byte[] chunk) {
    this.columnIndex = columnIndex;
    this.chunk = chunk;
  }

  @Override
  public int getColumnIndex() {
    return columnIndex;
  }

  /**
   * Returns the chunk of data.
   *
   * @return The chunk of data.
   */
  public byte[] getChunk() {
    return chunk;
  }
}
