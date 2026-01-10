package org.tdslib.javatdslib.tokens.colmetadata;

/**
 * Column metadata holder used by COLMETADATA token parser.
 */
public class ColumnMeta {
    private final int columnNumber;
    private final String name;
    private final byte dataType;
    private final int maxLength;
    private final short flags;
    private final int userType;
    private final byte[] collation; // may be null

    /**
     * Create a ColumnMeta instance.
     *
     * @param columnNumber 1-based column number
     * @param name         column name
     * @param dataType     TDS data type byte
     * @param maxLength    maximum length in bytes (or -1)
     * @param flags        column flags
     * @param userType     user type value
     * @param collation    optional 5-byte collation (may be null)
     */
    public ColumnMeta(final int columnNumber, final String name,
            final byte dataType, final int maxLength, final short flags,
            final int userType, final byte[] collation) {
        this.columnNumber = columnNumber;
        this.name = name;
        this.dataType = dataType;
        this.maxLength = maxLength;
        this.flags = flags;
        this.userType = userType;
        this.collation = collation != null ? collation.clone() : null;
    }

    /**
     * Column name accessor.
     *
     * @return column name
     */
    public String getName() {
        return name;
    }

    /**
     * Data type accessor.
     *
     * @return data type byte
     */
    public byte getDataType() {
        return dataType;
    }

    /**
     * Max length accessor.
     *
     * @return maximum length in bytes
     */
    public int getMaxLength() {
        return maxLength;
    }

    /**
     * Flags accessor.
     *
     * @return flags value
     */
    public short getFlags() {
        return flags;
    }

    /**
     * User type accessor.
     *
     * @return user type
     */
    public int getUserType() {
        return userType;
    }

    /**
     * Collation accessor. Returns the raw 5-byte collation or null.
     *
     * @return collation bytes or null
     */
    public byte[] getCollation() {
        return collation;
    }

    @Override
    public String toString() {
        return "Column " + columnNumber +
                ": " + name +
                " (type=0x" + Integer.toHexString(dataType & 0xFF) +
                ", maxLen=" + maxLength +
                ", nameLen=" + name.length() +
                ")";
    }
}