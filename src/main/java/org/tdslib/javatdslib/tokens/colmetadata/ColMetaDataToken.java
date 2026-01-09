package org.tdslib.javatdslib.tokens.colmetadata;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

import java.util.List;

public class ColMetaDataToken extends Token {
    private final short columnCount;
    private final List<ColumnMeta> columns;

    public  ColMetaDataToken(byte type, short columnCount, List<ColumnMeta> columns) {
        super(TokenType.fromValue(type));
        this.columnCount = columnCount;
        this.columns = columns;
    }

    public short getColumnCount() { return columnCount; }
    public List<ColumnMeta> getColumns() { return columns; }
}

