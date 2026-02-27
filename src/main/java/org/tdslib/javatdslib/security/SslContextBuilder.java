package org.tdslib.javatdslib.security;

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

public class SslContextBuilder {

  public static final Option<Boolean> TRUST_SERVER_CERTIFICATE = Option.valueOf("trustServerCertificate");
  public static final Option<String> TRUST_STORE = Option.valueOf("trustStore");
  public static final Option<String> TRUST_STORE_PASSWORD = Option.valueOf("trustStorePassword");

  public static SSLContext build(ConnectionFactoryOptions options) throws Exception {
    Object rawTrust = options.getValue(TRUST_SERVER_CERTIFICATE);
    boolean trustAll = false;

    if (rawTrust instanceof Boolean b) {
      trustAll = b;
    } else if (rawTrust != null) {
      trustAll = Boolean.parseBoolean(rawTrust.toString());
    }

    if (trustAll) {
      // Development Mode: Trust all certificates
      TrustManager[] trustAllCerts = new TrustManager[]{
          new X509TrustManager() {
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            @Override public void checkClientTrusted(X509Certificate[] certs, String authType) {}
            @Override public void checkServerTrusted(X509Certificate[] certs, String authType) {}
          }
      };
      SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
      sslContext.init(null, trustAllCerts, new SecureRandom());
      return sslContext;
    }

    // Production Mode: Strict Validation
    String trustStorePath = (String) options.getValue(TRUST_STORE);

    if (trustStorePath != null) {
      // User provided a specific TrustStore via R2DBC options
      String trustStorePassword = (String) options.getValue(TRUST_STORE_PASSWORD);
      char[] password = trustStorePassword != null ? trustStorePassword.toCharArray() : null;

      KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      try (InputStream is = new FileInputStream(trustStorePath)) {
        ks.load(is, password);
      }

      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);

      SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
      sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());
      return sslContext;
    }

    // Default strict validation using the standard JVM trust store
    return SSLContext.getDefault();
  }
}