// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.io.connection.tcp;

import org.tdslib.javatdslib.io.connection.ConnectionOptions;
import java.time.Duration;
import javax.net.ssl.HostnameVerifier;

/**
 * Connection options for Tcp connection.
 */
public class TcpConnectionOptions extends ConnectionOptions {
    /**
     * Local endpoint to use for the socket.
     * If this value is null then the local endpoint will be assigned by the operating system.
     */
    private String localEndpoint; // or InetSocketAddress

    /**
     * Connect timeout for the connection.
     * A value of Duration.ZERO indicates default operating system timeout. Default value is the operating system timeout.
     */
    private Duration connectTimeout = Duration.ofMillis(-1);

    /**
     * Receive timeout for the connection.
     * A value of Duration.ZERO indicates infinite timeout. Default value is 5 seconds.
     */
    private Duration receiveTimeout = Duration.ofSeconds(5);

    /**
     * Send timeout for the connection.
     * A value of Duration.ZERO indicates infinite timeout. Default value is 5 seconds.
     */
    private Duration sendTimeout = Duration.ofSeconds(5);

    /**
     * Optional hostname verifier for TLS.
     */
    private HostnameVerifier hostnameVerifier;

    /**
     * Optional hostname to be used for TLS server certificate name validation.
     */
    private String tlsCertificateHostname;

    public String getLocalEndpoint() {
        return localEndpoint;
    }

    public void setLocalEndpoint(String localEndpoint) {
        this.localEndpoint = localEndpoint;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Duration getReceiveTimeout() {
        return receiveTimeout;
    }

    public void setReceiveTimeout(Duration receiveTimeout) {
        this.receiveTimeout = receiveTimeout;
    }

    public Duration getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(Duration sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    public void setHostnameVerifier(HostnameVerifier hostnameVerifier) {
        this.hostnameVerifier = hostnameVerifier;
    }

    public String getTlsCertificateHostname() {
        return tlsCertificateHostname;
    }

    public void setTlsCertificateHostname(String tlsCertificateHostname) {
        this.tlsCertificateHostname = tlsCertificateHostname;
    }
}