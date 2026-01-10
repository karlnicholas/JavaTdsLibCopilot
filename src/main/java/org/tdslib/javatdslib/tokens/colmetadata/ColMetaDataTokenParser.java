package org.tdslib.javatdslib.tokens.colmetadata;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.tdslib.javatdslib.ConnectionContext;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenParser;

/**
 * Parser for COLMETADATA token (0x81) – column metadata.
 * Fully aligned with TDS 7.2+ (SQL Server 2005+) structure.
 * Tested against Wireshark decode showing two NVARCHAR columns:
 * "version" and "db".
 */
public class ColMetaDataTokenParser implements TokenParser {

    @Override
    public Token parse(final ByteBuffer payload, final byte tokenType,
            final ConnectionContext context, final QueryContext queryContext) {
        if (tokenType != (byte) 0x81) {
            throw new IllegalArgumentException(
                    "Expected COL_METADATA token (0x81), but got 0x"
                            + Integer.toHexString(tokenType & 0xFF));
        }

        // Number of columns (USHORT)
        final short columnCount = payload.getShort();

        final List<ColumnMeta> columns = new ArrayList<>(columnCount);

        for (int colIndex = 0; colIndex < columnCount; colIndex++) {
            // -----------------------------------------------------------------
            // 1. UserType (ULONG / 4 bytes) in TDS 7.2+
            // -----------------------------------------------------------------
            final int userType = payload.getInt();

            // -----------------------------------------------------------------
            // 2. Flags (USHORT / 2 bytes)
            // -----------------------------------------------------------------
            final short flags = payload.getShort();

            // -----------------------------------------------------------------
            // 3. TypeInfo
            // -----------------------------------------------------------------
            final byte dataType = payload.get(); // e.g. 0xE7 = NVARCHAR

            // Read type-specific info (we focus on variable-length string types
            // here)
            int maxLength = -1;
            byte[] collation = null;

            if (isVariableLengthStringType(dataType)) {
                // Max length in bytes (USHORT for NVARCHAR/VARCHAR)
                maxLength = payload.getShort() & 0xFFFF;

                // Collation (exactly 5 bytes) for character types
                collation = new byte[5];
                payload.get(collation);
            } else {
                // For fixed-length types or others → skip or handle as needed
                // (expand later if you need INT, DATETIME, etc.)
            }

            // -----------------------------------------------------------------
            // 4. Column name (USHORT char count + UTF-16LE)
            // -----------------------------------------------------------------
            final short nameLengthInChars = payload.get();
            String columnName = "";

            if (nameLengthInChars > 0) {
                if (nameLengthInChars > 512) { // safety check
                    throw new IllegalStateException(
                            "Suspiciously long column name: " + nameLengthInChars);
                }
                final byte[] nameBytes = new byte[nameLengthInChars * 2];
                payload.get(nameBytes);
                columnName = new String(nameBytes, StandardCharsets.UTF_16LE);
            }

            // -----------------------------------------------------------------
            // Create and store metadata
            // -----------------------------------------------------------------
            final ColumnMeta meta = new ColumnMeta(
                    colIndex + 1, // 1-based column number
                    columnName,
                    dataType,
                    maxLength,
                    flags,
                    userType,
                    collation
            );

            columns.add(meta);
        }

        final ColMetaDataToken token = new ColMetaDataToken(tokenType, columnCount,
                columns);
        queryContext.setColMetaDataToken(token);
        return token;
    }

    /**
     * Returns true for types that include max length + collation (e.g.
     * NVARCHAR, VARCHAR, NCHAR, CHAR).
     */
    private boolean isVariableLengthStringType(final byte dataType) {
        return dataType == (byte) 0xE7 // NVARCHAR
                || dataType == (byte) 0xEF // NCHAR
                || dataType == (byte) 0x27 // VARCHAR
                || dataType == (byte) 0x2F; // CHAR
    }

}