package org.tdslib.javatdslib.api;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.handshake.HandshakeOrchestrator;
import org.tdslib.javatdslib.security.SslConfiguration;
import org.tdslib.javatdslib.security.SslContextBuilder;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.DefaultConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import javax.net.ssl.SSLContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;

public class TdsConnectionFactory implements ConnectionFactory {
  private static final Logger logger = LoggerFactory.getLogger(TdsConnectionFactory.class);

  // Moved Custom Options to the API boundary
  public static final Option<Boolean> TRUST_SERVER_CERTIFICATE = Option.valueOf("trustServerCertificate");
  public static final Option<String> TRUST_STORE = Option.valueOf("trustStore");
  public static final Option<String> TRUST_STORE_PASSWORD = Option.valueOf("trustStorePassword");

  private final ConnectionFactoryOptions options;

  public TdsConnectionFactory(ConnectionFactoryOptions options) {
    this.options = options;
  }

  @Override
  public Publisher<? extends Connection> create() {
    return subscriber -> {
      subscriber.onSubscribe(new Subscription() {
        private final AtomicBoolean isCanceled = new AtomicBoolean(false);

        @Override
        public void request(long n) {
          if (n <= 0) return;

          // Offload the blocking I/O to a background thread
          CompletableFuture.runAsync(() -> {
            if (isCanceled.get()) return;

            try {
              String hostname = options.getRequiredValue(HOST).toString();
              int port = options.getValue(PORT) == null ? 1433 : (int) options.getValue(PORT);
              String username = (String) options.getValue(USER);
              String password = (String) options.getValue(PASSWORD);
              String database = (String) options.getValue(DATABASE);

              // Extract SSL Options
              Object rawTrust = options.getValue(TRUST_SERVER_CERTIFICATE);
              boolean trustAll = false;
              if (rawTrust instanceof Boolean b) {
                trustAll = b;
              } else if (rawTrust != null) {
                trustAll = Boolean.parseBoolean(rawTrust.toString());
              }

              // Build Internal Security Config
              SslConfiguration sslConfig = new SslConfiguration(
                  trustAll,
                  (String) options.getValue(TRUST_STORE),
                  (String) options.getValue(TRUST_STORE_PASSWORD)
              );

              SSLContext sslContext = SslContextBuilder.build(sslConfig);
              ConnectionContext context = new DefaultConnectionContext();
              TdsTransport transport = new TdsTransport(hostname, port, context);

              try {
                HandshakeOrchestrator orchestrator = new HandshakeOrchestrator();
                orchestrator.performHandshake(transport, context, sslContext, hostname, username, password, database);

                transport.enterAsyncMode();

                if (!isCanceled.get()) {
                  subscriber.onNext(new TdsConnection(transport, context));
                  subscriber.onComplete();
                } else {
                  transport.close(); // Clean up if canceled during handshake
                }

              } catch (Exception e) {
                logger.error("Handshake failed", e);
                transport.close();
                subscriber.onError(e);
              }
            } catch (Exception e) {
              subscriber.onError(e);
            }
          });
        }

        @Override
        public void cancel() {
          isCanceled.set(true);
        }
      });
    };
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return null;
  }
}