package org.tdslib.javatdslib;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.handshake.HandshakeOrchestrator;
import org.tdslib.javatdslib.security.SslContextBuilder;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.DefaultConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import javax.net.ssl.SSLContext;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsConnectionFactory implements ConnectionFactory {
  private static final Logger logger = LoggerFactory.getLogger(TdsConnectionFactory.class);

  private final ConnectionFactoryOptions options;

  public TdsConnectionFactory(ConnectionFactoryOptions options) {
    this.options = options;
  }

  @Override
  public Publisher<? extends Connection> create() {
    return subscriber -> {
      subscriber.onSubscribe(new Subscription() {
        @Override
        public void request(long n) {
          if (n > 0) {
            try {
              String hostname = options.getRequiredValue(HOST).toString();
              int port = options.getValue(PORT) == null ? 1433 : (int) options.getValue(PORT);
              String username = (String) options.getValue(USER);
              String password = (String) options.getValue(PASSWORD);
              String database = (String) options.getValue(DATABASE);

              // 1. Build SSL context based on Connection options
              SSLContext sslContext = SslContextBuilder.build(options);

              // 2. Setup transport infrastructure
              ConnectionContext context = new DefaultConnectionContext();
              TdsTransport transport = new TdsTransport(hostname, port, context);

              try {
                // 3. Delegate to the Orchestrator
                HandshakeOrchestrator orchestrator = new HandshakeOrchestrator();
                orchestrator.performHandshake(transport, context, sslContext, hostname, username, password, database);

                // 4. Hand off connected transport
                transport.enterAsyncMode();
                subscriber.onNext(new TdsConnection(transport, context));
                subscriber.onComplete();

              } catch (Exception e) {
                logger.error("Handshake failed", e);
                transport.close();
                subscriber.onError(e);
              }

            } catch (Exception e) {
              subscriber.onError(e);
            }
          }
        }

        @Override
        public void cancel() {
          // implementation
        }
      });
    };
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return null;
  }
}