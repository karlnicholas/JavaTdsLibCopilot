package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.protocol.TdsVersion;
import org.tdslib.javatdslib.protocol.CollationUtils;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Optional;

/**
 * Clean implementation of ConnectionContext.
 * Strictly holds session state without knowing about TCP or Sockets.
 */
public class DefaultConnectionContext implements ConnectionContext {

  private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionContext.class);

  private TdsVersion tdsVersion = TdsVersion.V7_4;
  private boolean utf8Negotiated = false;
  private String currentDatabase;
  private String currentLanguage = "us_english";
  private String currentCharset;
  private int packetSize = 4096;
  private byte[] currentCollationBytes = new byte[0];
  private boolean inTransaction;
  private String serverName;
  private String serverVersionString;
  private int spid;

  @Override
  public void resetToDefaults() {
    logger.trace("Resetting connection context to defaults.");
    this.utf8Negotiated = false;
    this.currentDatabase = null;
    this.currentLanguage = "us_english";
    this.currentCharset = null;
    this.packetSize = 4096;
    this.currentCollationBytes = new byte[0];
    this.inTransaction = false;
    this.spid = 0;
  }

  @Override public TdsVersion getTdsVersion() { return tdsVersion; }
  @Override public void setTdsVersion(TdsVersion version) { this.tdsVersion = version; }
  @Override public boolean isUnicodeEnabled() { return tdsVersion.ordinal() >= TdsVersion.V7_1.ordinal(); }

  @Override public boolean isUtf8Negotiated() { return utf8Negotiated; }
  @Override public void setUtf8Negotiated(boolean negotiated) { this.utf8Negotiated = negotiated; }
  @Override public String getCurrentDatabase() { return currentDatabase; }
  @Override public void setDatabase(String database) { this.currentDatabase = database; }
  @Override public String getCurrentLanguage() { return currentLanguage; }
  @Override public void setLanguage(String language) { this.currentLanguage = language; }
  @Override public String getCurrentCharset() { return currentCharset; }
  @Override public void setCharset(String charset) { this.currentCharset = charset; }
  @Override public int getCurrentPacketSize() { return packetSize; }
  @Override public void setPacketSize(int size) { this.packetSize = size; }
  @Override public byte[] getCurrentCollationBytes() { return Arrays.copyOf(currentCollationBytes, currentCollationBytes.length); }
  @Override public boolean isInTransaction() { return inTransaction; }
  @Override public void setInTransaction(boolean inTransaction) { this.inTransaction = inTransaction; }
  @Override public String getServerName() { return serverName; }
  @Override public void setServerName(String serverName) { this.serverName = serverName; }
  @Override public String getServerVersionString() { return serverVersionString; }
  @Override public void setServerVersionString(String versionString) { this.serverVersionString = versionString; }
  @Override public int getSpid() { return spid; }
  @Override public void setSpid(int spid) { logger.debug("Setting SPID to {}", spid); this.spid = spid; }
  @Override public Optional<Charset> getNonUnicodeCharset() { return CollationUtils.getCharsetFromCollation(currentCollationBytes); }
  @Override public void setCollationBytes(byte[] collationBytes) { this.currentCollationBytes = collationBytes != null ? collationBytes.clone() : null; }

 }