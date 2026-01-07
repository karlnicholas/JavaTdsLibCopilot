package org.tdslib.javatdslib.tokens.row;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

import java.util.List;

public class RowToken extends Token {
    private final List<byte[]> columnData;

    public RowToken(byte type, List<byte[]> columnData) {
        super(TokenType.fromValue(type));
        this.columnData = columnData;
    }

    public List<byte[]> getColumnData() { return columnData; }

    public byte[] getColumn(int index) {
        return index < columnData.size() ? columnData.get(index) : null;
    }
}