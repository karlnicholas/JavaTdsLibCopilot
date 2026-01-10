package org.tdslib.javatdslib.tokens.colmetadata;

import java.util.List;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Token representing column metadata (COLMETADATA token, 0x81).
 */
public class ColMetaDataToken extends Token {
    private final short columnCount;
    private final List<ColumnMeta> columns;

    /**
     * Create a COLMETADATA token instance.
     *
     * @param type        raw token byte value
     * @param columnCount number of columns described
     * @param columns     list of column metadata objects
     */
    public ColMetaDataToken(final byte type, final short columnCount,
            final List<ColumnMeta> columns) {
        super(TokenType.fromValue(type));
        this.columnCount = columnCount;
        this.columns = columns;
    }

    /**
     * Returns the number of columns described by this token.
     */
    public short getColumnCount() {
        return columnCount;
    }

    /**
     * Returns the list of column metadata objects.
     */
    public List<ColumnMeta> getColumns() {
        return columns;
    }
}
