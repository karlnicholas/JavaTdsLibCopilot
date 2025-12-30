// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.io.connection.tcp;

/**
 * TCP server endpoint information.
 */
public class TcpServerEndpoint {
    private final String hostname;
    private final int port;

    /**
     * The hostname of the server endpoint.
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * The port of the server endpoint.
     */
    public int getPort() {
        return port;
    }

    /**
     * Create a new endpoint with a hostname and port.
     */
    public TcpServerEndpoint(String hostname, int port) {
        this.hostname = hostname != null ? hostname : "";
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Specified port is invalid, outside valid range [0-65535]");
        }
        this.port = port;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TcpServerEndpoint that = (TcpServerEndpoint) obj;
        return port == that.port && hostname.equals(that.hostname);
    }

    @Override
    public int hashCode() {
        return hostname.hashCode() * 31 + port;
    }

    @Override
    public String toString() {
        return hostname + ":" + port;
    }
}