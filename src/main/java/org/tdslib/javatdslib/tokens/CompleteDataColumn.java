package org.tdslib.javatdslib.tokens;

public class CompleteDataColumn implements ColumnData {
  private final int columnIndex;
  private final byte[] data;

  public CompleteDataColumn(int columnIndex, byte[] data) {
    this.columnIndex = columnIndex;
    this.data = data;
  }
  @Override public int getColumnIndex() { return columnIndex; }
  public byte[] getData() { return data; }
}
