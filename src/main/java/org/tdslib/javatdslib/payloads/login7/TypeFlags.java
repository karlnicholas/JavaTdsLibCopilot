package org.tdslib.javatdslib.payloads.login7;

/**
 * Flags representing type options for the Login7 payload.
 */
public final class TypeFlags {

  /**
   * SQL type option.
   */
  public enum OptionSqlType {
    Default,
    TSQL
  }

  /**
   * OLE DB option.
   */
  public enum OptionOleDb {
    Off,
    On
  }

  /**
   * Access intent option.
   */
  public enum OptionAccessIntent {
    ReadWrite,
    ReadOnly
  }

  private static final int OptionSqlTypeBitIndex = 0x08;
  private static final int OptionOleDbBitIndex = 0x10;
  private static final int OptionAccesIntentBitIndex = 0x20;

  private byte value;

  /**
   * Construct default TypeFlags with default options.
   */
  public TypeFlags() {
    this.value = 0;
    setSqlType(OptionSqlType.Default);
    setOleDb(OptionOleDb.Off);
    setAccessIntent(OptionAccessIntent.ReadWrite);
  }

  /**
   * Construct TypeFlags from a raw byte value.
   *
   * @param value raw flags byte
   */
  public TypeFlags(final byte value) {
    this.value = value;
  }

  /**
   * Get the SQL type option.
   *
   * @return current SQL type option
   */
  public OptionSqlType getSqlType() {
    if ((value & OptionSqlTypeBitIndex) == OptionSqlTypeBitIndex) {
      return OptionSqlType.TSQL;
    }
    return OptionSqlType.Default;
  }

  /**
   * Set the SQL type option.
   *
   * @param t SQL type to set
   */
  public void setSqlType(final OptionSqlType t) {
    if (t == OptionSqlType.Default) {
      value &= (byte) (0xFF - OptionSqlTypeBitIndex);
    } else {
      value |= OptionSqlTypeBitIndex;
    }
  }

  /**
   * Get the OLE DB option.
   *
   * @return current OLE DB option
   */
  public OptionOleDb getOleDb() {
    if ((value & OptionOleDbBitIndex) == OptionOleDbBitIndex) {
      return OptionOleDb.On;
    }
    return OptionOleDb.Off;
  }

  /**
   * Set the OLE DB option.
   *
   * @param v OLE DB option to set
   */
  public void setOleDb(final OptionOleDb v) {
    if (v == OptionOleDb.Off) {
      value &= (byte) (0xFF - OptionOleDbBitIndex);
    } else {
      value |= OptionOleDbBitIndex;
    }
  }

  /**
   * Get the access intent option.
   *
   * @return current access intent option
   */
  public OptionAccessIntent getAccessIntent() {
    if ((value & OptionAccesIntentBitIndex) == OptionAccesIntentBitIndex) {
      return OptionAccessIntent.ReadOnly;
    }
    return OptionAccessIntent.ReadWrite;
  }

  /**
   * Set the access intent option.
   *
   * @param v access intent to set
   */
  public void setAccessIntent(final OptionAccessIntent v) {
    if (v == OptionAccessIntent.ReadWrite) {
      value &= (byte) (0xFF - OptionAccesIntentBitIndex);
    } else {
      value |= OptionAccesIntentBitIndex;
    }
  }

  /**
   * Convert flags to a byte.
   *
   * @return flags as byte
   */
  public byte toByte() {
    return value;
  }

  /**
   * Construct TypeFlags from a byte.
   *
   * @param b raw flags byte
   * @return new TypeFlags instance
   */
  public static TypeFlags fromByte(final byte b) {
    return new TypeFlags(b);
  }

  @Override
  public String toString() {
    return String.format("TypeFlags[value=0x%02X, SqlType=%s, OleDb=%s, AccessIntent=%s]",
        Byte.toUnsignedInt(value), getSqlType(), getOleDb(), getAccessIntent());
  }
}
