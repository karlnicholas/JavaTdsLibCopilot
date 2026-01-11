package org.tdslib.javatdslib;

/**
 * Represents the current session/connection state in a TDS connection.
 * Tracks environment changes (ENVCHANGE), login ack info, and other server-driven state.
 * <p>
 * Getters are used for query execution and client awareness.
 * Setters are called internally by ApplyingTokenVisitor on ENVCHANGE/LOGINACK.
 */
public interface ConnectionContext {

  // === TDS Protocol Version (set from LOGINACK) ===
  TdsVersion getTdsVersion();

  void setTdsVersion(TdsVersion version);

  // === Derived: Unicode support (TDS 7.0+) ===
  boolean isUnicodeEnabled();

  // === Database ===
  String getCurrentDatabase();

  void setDatabase(String database);

  // === Language ===
  String getCurrentLanguage();

  void setLanguage(String language);

  // === Character Set / Code Page (legacy) ===
  String getCurrentCharset();

  void setCharset(String charset);

  // === Packet Size (negotiated via ENVCHANGE) ===
  int getCurrentPacketSize();

  void setPacketSize(int size);

  // === SQL Collation (binary from ENVCHANGE type 7) ===
  byte[] getCurrentCollationBytes();

  void setCollationBytes(byte[] collationBytes);

  // === Transaction State (from BEGIN/COMMIT/ROLLBACK ENVCHANGE) ===
  boolean isInTransaction();

  void setInTransaction(boolean inTransaction);

  // === Server Info (from LOGINACK) ===
  String getServerName();

  void setServerName(String serverName);

  String getServerVersionString();

  void setServerVersionString(String versionString);

  // === Reset / Recovery ===

  /**
   * Resets session state to defaults (called on resetConnection flag or explicit reset).
   * Should clear database, transaction state, etc., but keep TDS version.
   */
  void resetToDefaults();

  // === Optional future extensions ===
  // void addRoutingInfo(RoutingInfo info);
  // byte[] getTransactionId();
  // void setTransactionId(byte[] id);
}