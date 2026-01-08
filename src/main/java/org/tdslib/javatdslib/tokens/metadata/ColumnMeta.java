package org.tdslib.javatdslib.tokens.metadata;

/**
 * Optional: Enhanced ColumnMeta class with more fields.
 * You can keep your original or extend it like this.
 */
public class ColumnMeta {
    private final int columnNumber;
    private final String name;
    private final byte dataType;
    private final int maxLength;
    private final short flags;
    private final int userType;
    private final byte[] collation; // may be null

    public ColumnMeta(int columnNumber, String name, byte dataType,
                      int maxLength, short flags, int userType, byte[] collation) {
        this.columnNumber = columnNumber;
        this.name = name;
        this.dataType = dataType;
        this.maxLength = maxLength;
        this.flags = flags;
        this.userType = userType;
        this.collation = collation != null ? collation.clone() : null;
    }

    // Getters...
    public String getName() { return name; }
    public byte getDataType() { return dataType; }
    public int getMaxLength() { return maxLength; }
    public short getFlags() { return flags; }
    public int getUserType() { return userType; }
    public byte[] getCollation() { return collation; }

    @Override
    public String toString() {
        return "Column " + columnNumber + ": " + name +
                " (type=0x" + Integer.toHexString(dataType & 0xFF) +
                ", maxLen=" + maxLength + ", nameLen=" + name.length() + ")";
    }
}