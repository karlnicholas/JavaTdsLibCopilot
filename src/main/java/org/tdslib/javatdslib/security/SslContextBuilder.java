package org.tdslib.javatdslib.security;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * Builds an SSLContext based on standard SSL configuration.
 */
public class SslContextBuilder {

  /**
   * Builds an SSLContext.
   *
   * @param config the generic SSL configuration
   * @return the configured SSLContext
   * @throws Exception if an error occurs during SSL context creation
   */
  public static SSLContext build(SslConfiguration config) throws Exception {

    if (config.trustServerCertificate()) {
      // Development Mode: Trust all certificates
      TrustManager[] trustAllCerts = new TrustManager[]{
          new X509TrustManager() {
            @Override
            public X509Certificate[] getAcceptedIssuers() {
              return new X509Certificate[0];
            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }
          }
      };
      SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
      sslContext.init(null, trustAllCerts, new SecureRandom());
      return sslContext;
    }

    // Production Mode: Strict Validation
    String trustStorePath = config.trustStorePath();

    if (trustStorePath != null) {
      // User provided a specific TrustStore
      String trustStorePassword = config.trustStorePassword();
      char[] password = trustStorePassword != null ? trustStorePassword.toCharArray() : null;

      KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      try (InputStream is = new FileInputStream(trustStorePath)) {
        ks.load(is, password);
      }

      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);

      SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
      sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());
      return sslContext;
    }

    // Default strict validation using the standard JVM trust store
    return SSLContext.getDefault();
  }
}