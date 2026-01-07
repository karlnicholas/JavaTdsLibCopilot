package org.tdslib.javatdslib.tokens.row;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;
import org.tdslib.javatdslib.tokens.metadata.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.metadata.ColumnMeta;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses standard ROW token (0xD1).
 * Assumes we have previous COL_METADATA to know types/lengths.
 * For simplicity, reads all columns as byte[] (raw data).
 */
public class RowTokenParser implements TokenParser {

    private final ColMetaDataToken lastMeta;  // pass from context or previous token

    public RowTokenParser(ColMetaDataToken lastMeta) {
        this.lastMeta = lastMeta;
    }

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context) {
        if (tokenType != (byte) 0xD1) {
            throw new IllegalArgumentException("Expected ROW (0xD1), got 0x" + Integer.toHexString(tokenType & 0xFF));
        }

        List<byte[]> columnData = new ArrayList<>();

        // For each column from previous metadata
        for (ColumnMeta col : lastMeta.getColumns()) {
            byte dataType = col.dataType;
            byte[] data;

            if (isFixedLength(dataType)) {
                data = new byte[getFixedLength(dataType)];
                payload.get(data);
            } else {
                // Variable length: USHORT length prefix (0xFFFF = NULL)
                int len = payload.getShort() & 0xFFFF;
                if (len == 0xFFFF) {
                    data = null;  // NULL
                } else {
                    data = new byte[len];
                    payload.get(data);
                }
            }

            columnData.add(data);
        }

        return new RowToken(tokenType, columnData);
    }

    private boolean isFixedLength(byte type) {
        // Add more as needed
        return type == 0x26 || type == 0x38 || type == 0x3E || type == 0x3F; // int, tinyint, float, etc.
    }

    private int getFixedLength(byte type) {
        switch (type) {
            case 0x26: return 4;  // int
            // ...
            default: return 0;
        }
    }
}