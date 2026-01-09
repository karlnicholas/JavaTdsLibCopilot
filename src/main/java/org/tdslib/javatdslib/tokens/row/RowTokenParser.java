package org.tdslib.javatdslib.tokens.row;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.colmetadata.ColumnMeta;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses standard ROW token (0xD1).
 * Assumes we have previous COL_METADATA to know types/lengths.
 * For simplicity, reads all columns as byte[] (raw data).
 */
public class RowTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context, QueryContext queryContext) {
        if (tokenType != (byte) 0xD1) {
            throw new IllegalArgumentException("Expected ROW (0xD1), got 0x" + Integer.toHexString(tokenType & 0xFF));
        }

        List<byte[]> columnData = new ArrayList<>();

        // For each column from previous metadata
        for (ColumnMeta col : queryContext.getColMetaDataToken().getColumns()) {
            byte dataType = col.getDataType();
            byte[] data;

            if (TdsDataTypes.isFixedLength(dataType)) {
                int length = TdsDataTypes.getFixedLength(dataType);
                if (length > 0) {
                    data = new byte[length];
                    payload.get(data);
                } else {
                    data = null; // Should not happen for fixed lengths
                }
            } else if (TdsDataTypes.isNullableFixedLength(dataType)) {
                // For nullable types like INTN (0x26), etc.: BYTE length prefix
                int len = payload.get() & 0xFF;
                if (len == 0) {
                    data = null; // NULL
                } else {
                    data = new byte[len];
                    payload.get(data);
                }
            } else {
                // Variable length: USHORT length prefix (0xFFFF = NULL)
                int len = payload.getShort() & 0xFFFF;
                if (len == 0xFFFF) {
                    data = null; // NULL
                } else {
                    data = new byte[len];
                    payload.get(data);
                }
            }

            columnData.add(data);
        }

        return new RowToken(tokenType, columnData);
    }
}