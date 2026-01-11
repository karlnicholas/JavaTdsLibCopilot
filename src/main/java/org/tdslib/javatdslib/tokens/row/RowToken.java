package org.tdslib.javatdslib.tokens.row;

import java.util.List;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * ROW token - contains raw column data for a single row.
 */
public class RowToken extends Token {
    private final List<byte[]> columnData;

    /**
     * Constructs a RowToken.
     *
     * @param type       raw token byte
     * @param columnData list of raw column byte arrays (may contain nulls)
     */
    public RowToken(final byte type, final List<byte[]> columnData) {
        super(TokenType.fromValue(type));
        this.columnData = columnData;
    }

    /** Returns the raw column data list (may contain nulls). */
    public List<byte[]> getColumnData() {
        return columnData;
    }

    /** Returns the raw data for the column at index or null if out of bounds. */
    @SuppressWarnings("unused")
    public byte[] getColumn(final int index) {
        if (index < columnData.size()) {
            return columnData.get(index);
        }
        return null;
    }
}
