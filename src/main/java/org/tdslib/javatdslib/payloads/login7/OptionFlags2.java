package org.tdslib.javatdslib.payloads.login7;

/**
 * Flags for Login7 option set 2.
 */
public final class OptionFlags2 {

  /**
   * Initial language handling.
   */
  public enum OptionInitLang {
    Warn,
    Fatal
  }

  /**
   * ODBC option.
   */
  public enum OptionOdbc {
    Off,
    On
  }

  /**
   * User type option.
   */
  public enum OptionUser {
    Normal,
    Server,
    RemUser,
    SqlRepl
  }

  /**
   * Integrated security option.
   */
  public enum OptionIntegratedSecurity {
    Off,
    On
  }

  private static final int OptionInitLangBitIndex = 0x01;
  private static final int OptionOdbcBitIndex = 0x02;
  private static final int OptionUserBitIndexServer = 0x10;
  private static final int OptionUserBitIndexRemUser = 0x20;
  private static final int OptionUserBitIndexSqlRepl = 0x40;
  private static final int OptionIntegratedSecurityBitIndex = 0x80;

  private byte value;

  /**
   * Construct default OptionFlags2 with sane defaults.
   */
  public OptionFlags2() {
    this.value = 0;
    setInitLang(OptionInitLang.Warn);
    setOdbc(OptionOdbc.Off);
    setUser(OptionUser.Normal);
    setIntegratedSecurity(OptionIntegratedSecurity.Off);
  }

  /**
   * Construct OptionFlags2 from a raw byte value.
   *
   * @param value raw flags byte
   */
  public OptionFlags2(final byte value) {
    this.value = value;
  }

  /**
   * Get the initial-language option.
   *
   * @return current initial-language option
   */
  public OptionInitLang getInitLang() {
    if ((value & OptionInitLangBitIndex) == OptionInitLangBitIndex) {
      return OptionInitLang.Fatal;
    }
    return OptionInitLang.Warn;
  }

  /**
   * Set the initial-language option.
   *
   * @param v option to set
   */
  public void setInitLang(final OptionInitLang v) {
    if (v == OptionInitLang.Fatal) {
      value |= OptionInitLangBitIndex;
    } else {
      value &= (byte) (0xFF - OptionInitLangBitIndex);
    }
  }

  /**
   * Get the ODBC option.
   *
   * @return current ODBC option
   */
  public OptionOdbc getOdbc() {
    if ((value & OptionOdbcBitIndex) == OptionOdbcBitIndex) {
      return OptionOdbc.On;
    }
    return OptionOdbc.Off;
  }

  /**
   * Set the ODBC option.
   *
   * @param v option to set
   */
  public void setOdbc(final OptionOdbc v) {
    if (v == OptionOdbc.On) {
      value |= OptionOdbcBitIndex;
    } else {
      value &= (byte) (0xFF - OptionOdbcBitIndex);
    }
  }

  /**
   * Get the user type option.
   *
   * @return current user option
   */
  public OptionUser getUser() {
    if ((value & OptionUserBitIndexServer) == OptionUserBitIndexServer) {
      return OptionUser.Server;
    }
    if ((value & OptionUserBitIndexRemUser) == OptionUserBitIndexRemUser) {
      return OptionUser.RemUser;
    }
    if ((value & OptionUserBitIndexSqlRepl) == OptionUserBitIndexSqlRepl) {
      return OptionUser.SqlRepl;
    }
    return OptionUser.Normal;
  }

  /**
   * Set the user type option.
   *
   * @param u user option to set
   */
  public void setUser(final OptionUser u) {
    value &= (byte) (0xFF - (OptionUserBitIndexServer
        | OptionUserBitIndexRemUser | OptionUserBitIndexSqlRepl));

    if (u == OptionUser.Server) {
      value |= OptionUserBitIndexServer;
    } else if (u == OptionUser.RemUser) {
      value |= OptionUserBitIndexRemUser;
    } else if (u == OptionUser.SqlRepl) {
      value |= OptionUserBitIndexSqlRepl;
    }
  }

  /**
   * Get the integrated-security option.
   *
   * @return current integrated-security option
   */
  public OptionIntegratedSecurity getIntegratedSecurity() {
    if ((value & OptionIntegratedSecurityBitIndex)
        == OptionIntegratedSecurityBitIndex) {
      return OptionIntegratedSecurity.On;
    }
    return OptionIntegratedSecurity.Off;
  }

  /**
   * Set the integrated-security option.
   *
   * @param v option to set
   */
  public void setIntegratedSecurity(final OptionIntegratedSecurity v) {
    if (v == OptionIntegratedSecurity.On) {
      value |= OptionIntegratedSecurityBitIndex;
    } else {
      value &= (byte) (0xFF - OptionIntegratedSecurityBitIndex);
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
   * Construct OptionFlags2 from a byte.
   *
   * @param b raw flags byte
   * @return new OptionFlags2 instance
   */
  public static OptionFlags2 fromByte(final byte b) {
    return new OptionFlags2(b);
  }

  @Override
  public String toString() {
    return String.format("OptionFlags2[value=0x%02X]", Byte.toUnsignedInt(value));
  }
}
