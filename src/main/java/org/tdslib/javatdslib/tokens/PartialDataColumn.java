package org.tdslib.javatdslib.tokens;

public class PartialDataColumn implements ColumnData {
  private final int columnIndex;
  private final byte[] chunk;
  private final boolean isLastChunk;

  public PartialDataColumn(int columnIndex, byte[] chunk, boolean isLastChunk) {
    this.columnIndex = columnIndex;
    this.chunk = chunk;
    this.isLastChunk = isLastChunk;
  }
  @Override public int getColumnIndex() { return columnIndex; }
  public byte[] getChunk() { return chunk; }
  public boolean isLastChunk() { return isLastChunk; }
}
