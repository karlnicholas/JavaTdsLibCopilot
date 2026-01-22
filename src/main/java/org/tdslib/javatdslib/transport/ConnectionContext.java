package org.tdslib.javatdslib.transport;

import org.tdslib.javatdslib.TdsVersion;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Represents the current session/connection state in a TDS connection.
 * Tracks environment changes (ENVCHANGE), login ack info, and other server-driven state.
 *
 * <p>Getters are used for query execution and client awareness.
 * Setters are called internally by {@code ApplyingTokenVisitor} on ENVCHANGE/LOGINACK.
 */
public interface ConnectionContext {

  // === TDS Protocol Version (set from LOGINACK) ===

  /**
   * Returns the negotiated TDS protocol version for this connection.
   *
   * @return the current {@link TdsVersion}
   */
  TdsVersion getTdsVersion();

  /**
   * Sets the negotiated TDS protocol version.
   *
   * @param version the {@link TdsVersion} to set
   */
  void setTdsVersion(TdsVersion version);

  // === Derived: Unicode support (TDS 7.0+) ===

  /**
   * Indicates whether Unicode (UTF-16/UTF-16LE) is enabled for this session.
   *
   * @return true if Unicode is enabled
   */
  boolean isUnicodeEnabled();

  // === Database ===

  /**
   * Returns the current default database for the session.
   *
   * @return current database name, or null if none
   */
  String getCurrentDatabase();

  /**
   * Sets the current default database for the session.
   *
   * @param database database name to set
   */
  void setDatabase(String database);

  // === Language ===

  /**
   * Returns the current language setting for the session.
   *
   * @return current language name
   */
  String getCurrentLanguage();

  /**
   * Sets the current language for the session.
   *
   * @param language language name to set
   */
  void setLanguage(String language);

  // === Character Set / Code Page (legacy) ===

  /**
   * Returns the currently selected character set / code page.
   *
   * @return charset name or null
   */
  String getCurrentCharset();

  /**
   * Sets the current character set / code page.
   *
   * @param charset charset name to set
   */
  void setCharset(String charset);

  // === Packet Size (negotiated via ENVCHANGE) ===

  /**
   * Returns the currently negotiated packet size.
   *
   * @return packet size in bytes
   */
  int getCurrentPacketSize();

  /**
   * Sets the current packet size.
   *
   * @param size packet size in bytes
   */
  void setPacketSize(int size);

  // === SQL Collation (binary from ENVCHANGE type 7) ===

  /**
   * Returns the binary collation bytes negotiated from the server.
   *
   * @return collation bytes, or null if not set
   */
  byte[] getCurrentCollationBytes();

  /**
   * Sets the binary collation bytes for this session.
   *
   * @param collationBytes collation bytes to set
   */
  void setCollationBytes(byte[] collationBytes);

  // === Transaction State (from BEGIN/COMMIT/ROLLBACK ENVCHANGE) ===

  /**
   * Indicates whether the session is currently inside a transaction.
   *
   * @return true if in transaction
   */
  boolean isInTransaction();

  /**
   * Sets the transaction state for the session.
   *
   * @param inTransaction true when inside a transaction
   */
  void setInTransaction(boolean inTransaction);

  // === Server Info (from LOGINACK) ===

  /**
   * Returns the server name reported by the server (LOGINACK).
   *
   * @return server name, or null if not available
   */
  String getServerName();

  /**
   * Sets the server name reported by the server.
   *
   * @param serverName server name to set
   */
  void setServerName(String serverName);

  /**
   * Returns the server version string reported in LOGINACK.
   *
   * @return server version string, or null if not available
   */
  String getServerVersionString();

  /**
   * Sets the server version string reported in LOGINACK.
   *
   * @param versionString version string to set
   */
  void setServerVersionString(String versionString);

  // === Reset / Recovery ===

  /**
   * Resets session state to defaults (called on resetConnection flag or explicit reset).
   * Should clear database, transaction state, etc., but keep TDS version.
   */
  void resetToDefaults();

  /**
   * Returns the server process id (SPID) assigned by the server for this connection.
   *
   * @return the SPID for this session, or a negative or zero value if not available
   */
  int getSpid();

  /**
   * Sets the server process id (SPID) assigned by the server for this connection.
   *
   * @param spid the SPID for this session; may be zero or negative if not available
   */
  void setSpid(int spid);
// Add these to the interface

  /**
   * Returns the Java Charset most appropriate for decoding non-Unicode (varchar/char) data
   * based on the current server collation (ENVCHANGE type 7).
   * Returns empty if unknown, Unicode-only, or no collation set.
   */
  Optional<Charset> getNonUnicodeCharset();

  /**
   * Returns UTF-16LE if Unicode is enabled (TDS 7.0+), otherwise falls back to non-Unicode charset.
   * This is usually what you want for reading strings from the server.
   */
  default Charset getEffectiveCharset() {
    if (isUnicodeEnabled()) {
      return StandardCharsets.UTF_16LE;  // TDS uses UTF-16LE for nvarchar/nchar
    }
    return getNonUnicodeCharset().orElse(StandardCharsets.ISO_8859_1); // fallback
  }
}
