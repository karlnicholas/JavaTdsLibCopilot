package org.tdslib.javatdslib;

import org.tdslib.javatdslib.tokens.metadata.ColumnMeta;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

// Simple inner class representing one result set
public class ResultSet {
    private int columnCount = -1;
    private List<ColumnMeta> columns = new ArrayList<>();
    private final List<List<byte[]>> rawRows = new ArrayList<>();
    private long rowCount = -1;  // ← changed to long to match DoneToken.getRowCount()

    public int getColumnCount() {
        return columnCount;
    }

    public List<List<byte[]>> getRawRows() {
        return new ArrayList<>(rawRows);
    }

    public void addRawRow(List<byte[]> row) {
        rawRows.add(row);
    }

    public long getRowCount() {
        return rowCount >= 0 ? rowCount : rawRows.size();
    }

    // Convenience: convert first column of first row to String
    public String getSingleStringResult() {
        if (rawRows.isEmpty() || rawRows.get(0).isEmpty()) return null;
        byte[] bytes = rawRows.get(0).get(0);
        return bytes != null ? new String(bytes, StandardCharsets.UTF_16LE).trim() : null;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    public void setColumns(List<ColumnMeta> columns) {
        this.columns = columns;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }
}
