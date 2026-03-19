package org.tdslib.javatdslib.api;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
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

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

/**
 * An R2DBC {@link ConnectionFactory} for creating connections to a TDS-based database. This factory
 * uses {@link ConnectionFactoryOptions} to configure connection details such as host, port, user,
 * and password, as well as custom SSL settings.
 */
public class TdsConnectionFactory implements ConnectionFactory {
  private static final Logger logger = LoggerFactory.getLogger(TdsConnectionFactory.class);

  // Moved Custom Options to the API boundary
  public static final Option<Boolean> TRUST_SERVER_CERTIFICATE =
      Option.valueOf("trustServerCertificate");
  public static final Option<String> TRUST_STORE = Option.valueOf("trustStore");
  public static final Option<String> TRUST_STORE_PASSWORD = Option.valueOf("trustStorePassword");

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

      SslConfiguration sslConfig = new SslConfiguration(
          Boolean.parseBoolean(String.valueOf(options.getValue(TRUST_SERVER_CERTIFICATE))),
          (String) options.getValue(TRUST_STORE),
          (String) options.getValue(TRUST_STORE_PASSWORD));

      try {
        SSLContext sslContext = SslContextBuilder.build(sslConfig);
        ConnectionContext context = new DefaultConnectionContext();
        TdsTransport transport = new TdsTransport(hostname, port, context);

        HandshakeOrchestrator orchestrator = new HandshakeOrchestrator();
        orchestrator.performHandshake(transport, context, sslContext, hostname, username, password, database);

        transport.enterAsyncMode();

        // Emit the connection and handle cancellation
        TdsConnection connection = new TdsConnection(transport, context);
        sink.onCancel(() -> {
          try { transport.close(); } catch (IOException ignored) {}
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
    return null;
  }
}
