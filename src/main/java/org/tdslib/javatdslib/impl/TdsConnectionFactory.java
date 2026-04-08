package org.tdslib.javatdslib.impl;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.handshake.HandshakeOrchestrator;
import org.tdslib.javatdslib.security.SslConfiguration;
import org.tdslib.javatdslib.security.SslContextBuilder;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.DefaultConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.tdslib.javatdslib.api.TdsLibOptions.TRUST_SERVER_CERTIFICATE;
import static org.tdslib.javatdslib.api.TdsLibOptions.TRUST_STORE;
import static org.tdslib.javatdslib.api.TdsLibOptions.TRUST_STORE_PASSWORD;

/**
 * An R2DBC {@link ConnectionFactory} for creating connections to a TDS-based database. This factory
 * uses {@link ConnectionFactoryOptions} to configure connection details such as host, port, user,
 * and password, as well as custom SSL settings.
 */
public class TdsConnectionFactory implements ConnectionFactory {
  private static final Logger logger = LoggerFactory.getLogger(TdsConnectionFactory.class);

  private final ConnectionFactoryOptions options;

  /**
   * Constructs a new TdsConnectionFactory with the specified options.
   *
   * @param options The configuration options for creating connections.
   */
  public TdsConnectionFactory(ConnectionFactoryOptions options) {
    this.options = options;
  }

  @Override
  public Publisher<? extends Connection> create() {
    return Mono.<Connection>create(sink -> {
      String hostname = options.getRequiredValue(HOST).toString();
      int port = options.getValue(PORT) == null ? 1433 : (int) options.getValue(PORT);
      String username = (String) options.getValue(USER);
      String password = (String) options.getValue(PASSWORD);
      String database = (String) options.getValue(DATABASE);

      // --- NEW: Extract the CONNECT_TIMEOUT option (Default to 15 seconds) ---
      Duration timeoutOption = (Duration) options.getValue(CONNECT_TIMEOUT);
      int connectTimeoutMs = timeoutOption != null ? (int) timeoutOption.toMillis() : 15_000;

      SslConfiguration sslConfig = new SslConfiguration(
          Boolean.parseBoolean(String.valueOf(options.getValue(TRUST_SERVER_CERTIFICATE))),
          (String) options.getValue(TRUST_STORE),
          (String) options.getValue(TRUST_STORE_PASSWORD));

      try {
        SSLContext sslContext = SslContextBuilder.build(sslConfig);
        ConnectionContext context = new DefaultConnectionContext();

        // --- NEW: Pass the timeout parameter to the transport ---
        TdsTransport transport = new TdsTransport(hostname, port, connectTimeoutMs, context);

        HandshakeOrchestrator orchestrator = new HandshakeOrchestrator();
        orchestrator.performHandshake(
            transport, context, sslContext, hostname, username, password, database);

        transport.enterAsyncMode();

        // Emit the connection and handle cancellation
        TdsConnection connection = new TdsConnection(transport, context);
        sink.onCancel(() -> {
          try {
            transport.close();
          } catch (IOException ignored) {
            // Ignored
          }
        });
        sink.success(connection);

      } catch (Exception e) {
        logger.error("Handshake failed", e);
        sink.error(e);
      }
    }).subscribeOn(Schedulers.boundedElastic()); // Crucial for blocking I/O during handshake
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return () -> "Microsoft SQL Server";
  }
}