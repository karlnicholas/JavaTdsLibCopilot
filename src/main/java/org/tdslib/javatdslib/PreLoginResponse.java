package org.tdslib.javatdslib;

/**
 * Parsed response from the PreLogin message.
 * Contains SQL Server version information and encryption settings.
 */
public class PreLoginResponse {

  private int sqlVersionMajor = 0;
  private int sqlVersionMinor = 0;
  private int sqlBuildNumber = 0;
  private byte encryption = 0;          // 0=off, 1=on, 2=requested, 3=required
  private int negotiatedPacketSize = 4096; // default, updated if server specifies

  /**
   * Sets the parsed SQL Server version components.
   *
   * @param major major version number.
   * @param minor minor version number.
   * @param build build number.
   */
  public void setVersion(int major, int minor, int build) {
    this.sqlVersionMajor = major;
    this.sqlVersionMinor = minor;
    this.sqlBuildNumber = build;
  }

  /**
   * Sets the encryption flag received from the server.
   *
   * @param enc encryption flag (0=off, 1=on, 2=requested, 3=required).
   */
  public void setEncryption(byte enc) {
    this.encryption = enc;
  }

  /**
   * Sets the negotiated packet size if within allowed range.
   *
   * @param size negotiated packet size to apply.
   */
  public void setNegotiatedPacketSize(int size) {
    if (size >= 512 && size <= 32767) {
      this.negotiatedPacketSize = size;
    } // else keep default
  }

  // Getters
  public int getSqlVersionMajor() {
    return sqlVersionMajor;
  }

  public int getSqlVersionMinor() {
    return sqlVersionMinor;
  }

  public int getSqlBuildNumber() {
    return sqlBuildNumber;
  }

  public String getVersionString() {
    return sqlVersionMajor + "." + sqlVersionMinor + "." + sqlBuildNumber;
  }

  public byte getEncryption() {
    return encryption;
  }

  public int getNegotiatedPacketSize() {
    return negotiatedPacketSize;
  }

//  /**
//   * Does the server require or request encryption.
//   * Useful to decide whether to start TLS handshake.
//   *
//   * @return true if encryption is on or required.
//   */
//  public boolean requiresEncryption() {
//    return encryption == 1 || encryption == 3; // on or required
//  }
//
//  /**
//   * Does the server support encryption.
//   *
//   * @return true if encryption is anything except off.
//   */
//  public boolean supportsEncryption() {
//    return encryption != 0; // anything except off
//  }

  @Override
  public String toString() {
    return "PreLoginResponse{"
        + "version=" + getVersionString()
        + ", encryption=" + encryption
        + ", packetSize=" + negotiatedPacketSize
        + '}';
  }
}
