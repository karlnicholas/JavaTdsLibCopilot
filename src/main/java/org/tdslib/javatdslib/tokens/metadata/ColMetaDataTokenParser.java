package org.tdslib.javatdslib.tokens.metadata;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses COLMETADATA token (0x81) – column metadata.
 * Minimal version: just counts columns and stores basic info.
 */
public class ColMetaDataTokenParser implements TokenParser {

    @Override
    public Token parse(ByteBuffer payload, byte tokenType, ConnectionContext context, QueryContext queryContext) {
        if (tokenType != (byte) 0x81) {
            throw new IllegalArgumentException("Expected COL_METADATA (0x81), got 0x" + Integer.toHexString(tokenType & 0xFF));
        }

        short count = payload.getShort();  // USHORT – number of columns

        List<ColumnMeta> columns = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            // Skip UserType (USHORT or ULONG depending on TDS version, we take 4 bytes for safety)
            payload.getInt();  // UserType (4 bytes in TDS 7.2+)

            // Flags (2 bytes)
            short flags = payload.getShort();

            // TypeInfo – varies by data tokenType
            byte dataType = payload.get();  // TDS tokenType byte

            // Minimal TypeInfo handling (expand later for precision/scale/maxlen/collation)
            int maxLen = getTypeMaxLength(dataType, payload);  // helper below

            // Collation (5 bytes) if string tokenType
            byte[] collation = null;
            if (isStringType(dataType)) {
                collation = new byte[5];
                payload.get(collation);
            }

            // Column name (USHORT length + Unicode chars)
            short nameLen = payload.getShort();
            String name = "";
            if (nameLen > 0) {
                byte[] nameBytes = new byte[nameLen * 2];
                payload.get(nameBytes);
                name = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_16LE);
            }

            columns.add(new ColumnMeta(i + 1, name, dataType, maxLen, flags));
        }

        ColMetaDataToken colMetaDataToken = new ColMetaDataToken(tokenType, count, columns);
        queryContext.setColMetaDataToken(colMetaDataToken);
        return colMetaDataToken;
    }

    // Quick helper to skip/read basic TypeInfo length
    private int getTypeMaxLength(byte dataType, ByteBuffer buf) {
        switch (dataType) {
            case 0x27: case 0x2F: // VARCHAR, CHAR
            case 0x25: case 0x2D: // VARBINARY, BINARY
            case (byte) 0xE7: case (byte) 0xEF: // NVARCHAR, NCHAR (TDS 7+)
                return buf.getShort() & 0xFFFF;  // USHORT max length
            case 0x22: case 0x23: // TEXT, IMAGE (old)
            case (byte) 0xA5: case (byte) 0xA7: // NTEXT, NVARCHAR(MAX?)
                return buf.getInt();  // ULONG PLP (partially length prefixed)
            default:
                return -1; // fixed length or not handled
        }
    }

    private boolean isStringType(byte type) {
        return type == (byte) 0xE7 || type == (byte) 0xEF || type == (byte) 0x27 || type == (byte) 0x2F;
    }
}