package org.tdslib.javatdslib.tokens;

public class PartialDataColumn implements ColumnData {
  private final int columnIndex;
  private final byte[] chunk;

  public PartialDataColumn(int columnIndex, byte[] chunk) {
    this.columnIndex = columnIndex;
    this.chunk = chunk;
  }
  @Override public int getColumnIndex() { return columnIndex; }
  public byte[] getChunk() { return chunk; }
}
