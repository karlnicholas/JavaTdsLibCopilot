package org.tdslib.javatdslib.security;

/**
 * Standard configuration for SSL/TLS settings, decoupled from R2DBC.
 */
public record SslConfiguration(
    boolean trustServerCertificate,
    String trustStorePath,
    String trustStorePassword
) {}