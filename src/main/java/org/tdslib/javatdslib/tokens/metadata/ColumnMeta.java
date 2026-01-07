package org.tdslib.javatdslib.tokens.metadata;

// Simple DTO for each column
public class ColumnMeta {
    public final int ordinal;
    public final String name;
    public final byte dataType;
    public final int maxLength;
    public final short flags;

    public ColumnMeta(int ordinal, String name, byte dataType, int maxLength, short flags) {
        this.ordinal = ordinal;
        this.name = name;
        this.dataType = dataType;
        this.maxLength = maxLength;
        this.flags = flags;
    }
}