package org.tdslib.javatdslib.tokens.colmetadata;

import org.tdslib.javatdslib.tokens.TypeInfo;

/**
 * Column metadata holder used by COLMETADATA token parser.
 */
public class ColumnMeta {
  private final int columnNumber;
  private final String name;
  private final int userType;
  private final short flags;
  private final TypeInfo typeInfo; // Replaces individual fields

  public ColumnMeta(final int columnNumber, final String name,
                    final int userType, final short flags, final TypeInfo typeInfo) {
    this.columnNumber = columnNumber;
    this.name = name;
    this.userType = userType;
    this.flags = flags;
    this.typeInfo = typeInfo;
  }

  public String getName() { return name; }
  public int getUserType() { return userType; }
  public short getFlags() { return flags; }

  // Delegates
  public byte getDataType() { return (byte) typeInfo.getTdsType().byteVal; }
  public int getMaxLength() { return typeInfo.getMaxLength(); }
  public byte getScale() { return typeInfo.getScale(); }
  public byte getPrecision() { return typeInfo.getPrecision(); }
  public byte[] getCollation() { return typeInfo.getCollation(); }

  public TypeInfo getTypeInfo() { return typeInfo; }

  @Override
  public String toString() {
    return String.format("Column %d: %s (type=%s, maxLen=%d)",
        columnNumber, name, typeInfo.getTdsType(), typeInfo.getMaxLength());
  }
}