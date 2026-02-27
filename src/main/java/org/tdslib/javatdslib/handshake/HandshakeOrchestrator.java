package org.tdslib.javatdslib.handshake;

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.LoginResponse;
import org.tdslib.javatdslib.PreLoginResponse;
import org.tdslib.javatdslib.tokens.visitors.LoginVisitor;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import javax.net.ssl.SSLContext;

public class HandshakeOrchestrator {
  private static final Logger logger = LoggerFactory.getLogger(HandshakeOrchestrator.class);

  private final PreLoginPhase preLoginPhase = new PreLoginPhase();
  private final Login7Phase login7Phase = new Login7Phase();

  public void performHandshake(TdsTransport transport, ConnectionContext context, SSLContext sslContext,
                               String hostname, String username, String password, String database) throws Exception {

    // 1. Pre-Login
    PreLoginResponse preLoginResponse = preLoginPhase.execute(transport);
    context.setPacketSize(preLoginResponse.getNegotiatedPacketSize());

    // 2. Encryption Toggle
    int serverEncryption = preLoginResponse.getEncryption();
    if (serverEncryption == 0x00 || serverEncryption == 0x01) {
      transport.tlsHandshake(sslContext);
    }

    // 3. Login7 Auth
    LoginVisitor loginVisitor = login7Phase.execute(transport, context, hostname, username, password, database);

    if (!loginVisitor.isSuccess()) {
      throw new R2dbcNonTransientResourceException(
          loginVisitor.getErrorMessage() != null ? loginVisitor.getErrorMessage() : "Login Failed"
      );
    }
    transport.tlsComplete();
  }
}