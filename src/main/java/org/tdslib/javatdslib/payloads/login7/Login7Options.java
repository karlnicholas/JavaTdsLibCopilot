package org.tdslib.javatdslib.payloads.login7;

import java.time.Instant;
import java.time.ZoneId;

import org.tdslib.javatdslib.TdsConstants;
import org.tdslib.javatdslib.TdsVersion;

/**
 * Configuration options used when building a TDS LOGIN7 payload.
 */
public class Login7Options {
  private TdsVersion tdsVersion;
  private int packetSize;
  private long clientProgVer;
  private long clientPid;
  private long connectionId;
  private int clientTimeZone;
  private long clientLcid;

  /**
   * Create default Login7 options.
   */
  public Login7Options() {
    this.tdsVersion = TdsVersion.V7_4;
    this.packetSize = TdsConstants.DEFAULT_PACKET_SIZE;
    this.clientProgVer = 0;
    this.clientPid = ProcessHandle.current().pid();
    this.connectionId = 0;
    this.clientTimeZone = ZoneId.systemDefault()
        .getRules()
        .getOffset(Instant.now())
        .getTotalSeconds() / 60;

    // FIXED: Use a valid LCID (0x0409 = en-US) instead of hashCode()
    this.clientLcid = 0x0409;
  }

  public TdsVersion getTdsVersion() {
    return tdsVersion;
  }

  public void setTdsVersion(final TdsVersion v) {
    this.tdsVersion = v;
  }

  public int getPacketSize() {
    return packetSize;
  }

  public void setPacketSize(final int s) {
    this.packetSize = s;
  }

  public long getClientProgVer() {
    return clientProgVer;
  }

  public void setClientProgVer(final long v) {
    this.clientProgVer = v;
  }

  public long getClientPid() {
    return clientPid;
  }

  public void setClientPid(final long p) {
    this.clientPid = p;
  }

  public long getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(final long id) {
    this.connectionId = id;
  }

  public int getClientTimeZone() {
    return clientTimeZone;
  }

  public void setClientTimeZone(final int tz) {
    this.clientTimeZone = tz;
  }

  public long getClientLcid() {
    return clientLcid;
  }

  public void setClientLcid(final long lcid) {
    this.clientLcid = lcid;
  }
}
