// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.io.connection;

import org.tdslib.javatdslib.TdsConstants;
import org.tdslib.javatdslib.TdsVersion;

/**
 * Tds client connection options.
 */
public class ConnectionOptions {
    private int packetSize;
    private TdsVersion tdsVersion;
    /**
     * Optional routing hint supplied by server via ENVCHANGE ROUTING.
     */
    private String routingHint;

    /**
     * Packet size in bytes.
     * Greater than {@link TdsConstants#PACKET_HEADER_LENGTH}.
     */
    public int getPacketSize() {
        return packetSize;
    }

    public void setPacketSize(int packetSize) {
        if (packetSize <= TdsConstants.PACKET_HEADER_LENGTH) {
            throw new IllegalArgumentException("Invalid packet size, must be greater than Packet header length.");
        }
        this.packetSize = packetSize;
    }

    /**
     * TDS Protocol version.
     */
    public TdsVersion getTdsVersion() {
        return tdsVersion;
    }

    public void setTdsVersion(TdsVersion tdsVersion) {
        this.tdsVersion = tdsVersion;
    }

    /**
     * Creates a new connection options with default values.
     * {@link #getPacketSize()} = {@link TdsConstants#DEFAULT_PACKET_SIZE},
     * {@link #getTdsVersion()} = {@link TdsVersion#V7_4}.
     */
    public ConnectionOptions() {
        this.packetSize = TdsConstants.DEFAULT_PACKET_SIZE;
        this.tdsVersion = TdsVersion.V7_4;
    }

    public String getRoutingHint() {
        return routingHint;
    }

    public void setRoutingHint(String routingHint) {
        this.routingHint = routingHint;
    }
}