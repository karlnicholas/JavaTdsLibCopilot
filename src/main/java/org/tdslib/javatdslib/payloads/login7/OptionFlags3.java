package org.tdslib.javatdslib.payloads.login7;

/**
 * Flags for Login7 option set 3.
 */
public final class OptionFlags3 {

  /**
   * Whether the client is requesting a password change.
   */
  public enum OptionChangePassword {
    No,
    Yes
  }

  private static final int OptionChangePasswordBitIndex = 0x01;
  private static final int OptionBinaryXmlBitIndex = 0x02;
  private static final int OptionSpawnUserInstanceBitIndex = 0x04;
  private static final int OptionUnkownCollationHandlingBitIndex = 0x08;
  private static final int OptionExtensionUsedBitIndex = 0x10;

  private byte value;

  /**
   * Construct default OptionFlags3 with sane defaults.
   */
  public OptionFlags3() {
    this.value = 0;
    setChangePassword(OptionChangePassword.No);
    setBinaryXml(false);
    setSpawnUserInstance(false);
    setUnknownCollationHandling(true);
    // Default to FALSE. Only enable if we actually have extensions.
    setExtensionUsed(false);
  }

  /**
   * Construct OptionFlags3 from a raw byte value.
   *
   * @param value raw flags byte
   */
  public OptionFlags3(final byte value) {
    this.value = value;
  }

  /**
   * Get the change-password option.
   *
   * @return current change-password option
   */
  public OptionChangePassword getChangePassword() {
    if ((value & OptionChangePasswordBitIndex) == OptionChangePasswordBitIndex) {
      return OptionChangePassword.Yes;
    }
    return OptionChangePassword.No;
  }

  /**
   * Set the change-password option.
   *
   * @param v option to set
   */
  public void setChangePassword(final OptionChangePassword v) {
    if (v == OptionChangePassword.No) {
      value &= (byte) (0xFF - OptionChangePasswordBitIndex);
    } else {
      value |= OptionChangePasswordBitIndex;
    }
  }

  /**
   * Whether Binary XML is enabled.
   *
   * @return true when Binary XML bit is set
   */
  public boolean isBinaryXml() {
    return (value & OptionBinaryXmlBitIndex) == OptionBinaryXmlBitIndex;
  }

  /**
   * Set Binary XML flag.
   *
   * @param v true to enable Binary XML
   */
  public void setBinaryXml(final boolean v) {
    if (v) {
      value |= OptionBinaryXmlBitIndex;
    } else {
      value &= (byte) (0xFF - OptionBinaryXmlBitIndex);
    }
  }

  /**
   * Whether spawn user instance is requested.
   *
   * @return true when spawn user instance bit is set
   */
  public boolean isSpawnUserInstance() {
    return (value & OptionSpawnUserInstanceBitIndex) == OptionSpawnUserInstanceBitIndex;
  }

  /**
   * Set spawn user instance flag.
   *
   * @param v true to enable spawn user instance
   */
  public void setSpawnUserInstance(final boolean v) {
    if (v) {
      value |= OptionSpawnUserInstanceBitIndex;
    } else {
      value &= (byte) (0xFF - OptionSpawnUserInstanceBitIndex);
    }
  }

  /**
   * Whether unknown collation handling is enabled.
   *
   * @return true when unknown collation handling bit is set
   */
  public boolean isUnknownCollationHandling() {
    return (value & OptionUnkownCollationHandlingBitIndex)
        == OptionUnkownCollationHandlingBitIndex;
  }

  /**
   * Set unknown collation handling flag.
   *
   * @param v true to enable unknown collation handling
   */
  public void setUnknownCollationHandling(final boolean v) {
    if (v) {
      value |= OptionUnkownCollationHandlingBitIndex;
    } else {
      value &= (byte) (0xFF - OptionUnkownCollationHandlingBitIndex);
    }
  }

  /**
   * Whether extension features are used.
   *
   * @return true when extension-used bit is set
   */
  public boolean isExtensionUsed() {
    return (value & OptionExtensionUsedBitIndex) == OptionExtensionUsedBitIndex;
  }

  /**
   * Set extension-used flag.
   *
   * @param v true to indicate extensions are used
   */
  public void setExtensionUsed(final boolean v) {
    if (v) {
      value |= OptionExtensionUsedBitIndex;
    } else {
      value &= (byte) (0xFF - OptionExtensionUsedBitIndex);
    }
  }

  /**
   * Convert flags to a byte.
   *
   * @return flags as a raw byte
   */
  public byte toByte() {
    return value;
  }

  /**
   * Construct OptionFlags3 from a byte.
   *
   * @param b raw flags byte
   * @return new OptionFlags3 instance
   */
  public static OptionFlags3 fromByte(final byte b) {
    return new OptionFlags3(b);
  }

  @Override
  public String toString() {
    return String.format("OptionFlags3[value=0x%02X]", Byte.toUnsignedInt(value));
  }
}
