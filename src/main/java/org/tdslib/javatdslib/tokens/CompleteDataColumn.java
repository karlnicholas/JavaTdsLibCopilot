package org.tdslib.javatdslib.tokens;

/**
 * Represents a completely fetched data column.
 */
public class CompleteDataColumn implements ColumnData {
  private final int columnIndex;
  private final byte[] data;

  /**
   * Constructs a new CompleteDataColumn.
   *
   * @param columnIndex The index of the column.
   * @param data        The complete column data as a byte array.
   */
  public CompleteDataColumn(int columnIndex, byte[] data) {
    this.columnIndex = columnIndex;
    this.data = data;
  }

  @Override
  public int getColumnIndex() {
    return columnIndex;
  }

  /**
   * Returns the complete column data.
   *
   * @return The complete column data as a byte array.
   */
  public byte[] getData() {
    return data;
  }
}
