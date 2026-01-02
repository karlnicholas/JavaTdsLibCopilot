// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.payloads.login7;

import org.tdslib.javatdslib.TdsConstants;
import org.tdslib.javatdslib.TdsVersion;

public class Login7Options {
    private TdsVersion tdsVersion;
    private int packetSize;
    private long clientProgVer;
    private long clientPid;
    private long connectionId;
    private int clientTimeZone;
    private long clientLcid;

    public Login7Options() {
        this.tdsVersion = TdsVersion.V7_4;
        this.packetSize = TdsConstants.DEFAULT_PACKET_SIZE;
        this.clientProgVer = 0;
        this.clientPid = ProcessHandle.current().pid();
        this.connectionId = 0;
        this.clientTimeZone = java.time.ZoneId.systemDefault().getRules().getOffset(java.time.Instant.now()).getTotalSeconds() / 60;

        // FIXED: Use a valid LCID (0x0409 = en-US) instead of hashCode()
        this.clientLcid = 0x0409;
    }

    // ... (Getters/Setters unchanged) ...
    public TdsVersion getTdsVersion() { return tdsVersion; }
    public void setTdsVersion(TdsVersion v) { this.tdsVersion = v; }
    public int getPacketSize() { return packetSize; }
    public void setPacketSize(int s) { this.packetSize = s; }
    public long getClientProgVer() { return clientProgVer; }
    public void setClientProgVer(long v) { this.clientProgVer = v; }
    public long getClientPid() { return clientPid; }
    public void setClientPid(long p) { this.clientPid = p; }
    public long getConnectionId() { return connectionId; }
    public void setConnectionId(long id) { this.connectionId = id; }
    public int getClientTimeZone() { return clientTimeZone; }
    public void setClientTimeZone(int tz) { this.clientTimeZone = tz; }
    public long getClientLcid() { return clientLcid; }
    public void setClientLcid(long lcid) { this.clientLcid = lcid; }
}