package org.tdslib.javatdslib.handshake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.InboundTdsPacket;
import org.tdslib.javatdslib.packets.OutboundTdsMessage;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.payloads.login7.Login7Options;
import org.tdslib.javatdslib.payloads.login7.Login7Payload;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.tokens.TokenParserRegistry;
import org.tdslib.javatdslib.tokens.visitors.CompositeTokenVisitor;
import org.tdslib.javatdslib.tokens.visitors.EnvChangeVisitor;
import org.tdslib.javatdslib.tokens.visitors.LoginVisitor;
import org.tdslib.javatdslib.tokens.visitors.MessageVisitor;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Handles the LOGIN7 phase of the TDS handshake.
 */
public class Login7Phase {
  private static final Logger logger = LoggerFactory.getLogger(Login7Phase.class);

  /**
   * Executes the Login7 phase.
   *
   * @param transport the transport layer
   * @param context   the connection context
   * @param hostname  the client hostname
   * @param database  the database to connect to
   * @param username  the username
   * @param password  the password
   * @return the LoginVisitor containing the login response details
   */
  public LoginVisitor execute(TdsTransport transport, ConnectionContext context,
                              String hostname, String database, String username, String password)
  throws Exception {
    logger.debug("Starting Login7 phase");

    Login7Options l7Opts = new Login7Options();
    Login7Payload login7Payload = new Login7Payload(l7Opts);
    login7Payload.hostname = hostname;
    login7Payload.database = database;
    login7Payload.username = username;
    login7Payload.password = password;

    OutboundTdsMessage login7Msg = OutboundTdsMessage.createRequest(PacketType.LOGIN7,
        Mono.just(login7Payload.buildBuffer()));

    if (transport.isTlsActive()) {
      transport.sendMessageEncrypted(login7Msg);
    } else {
      transport.sendMessageDirect(login7Msg);
    }

    List<InboundTdsPacket> loginResponseMsgs = transport.receiveFullResponse();
    return processLoginResponse(transport, context, loginResponseMsgs);
  }

  private LoginVisitor processLoginResponse(TdsTransport transport, ConnectionContext context,
                                            List<InboundTdsPacket> packets) {
    org.tdslib.javatdslib.tokens.visitors.LoginVisitor loginVisitor = new LoginVisitor();

    // Compose the Pipeline for Authentication
    CompositeTokenVisitor pipeline = new CompositeTokenVisitor(
        new EnvChangeVisitor(context),
        new MessageVisitor(null), // Terminal streams not needed during sync login
        loginVisitor
    );

    TokenDispatcher tokenDispatcher = new TokenDispatcher(TokenParserRegistry.DEFAULT);

    for (InboundTdsPacket msg : packets) {
      context.setSpid(msg.getSpid());
      tokenDispatcher.processMessage(msg, context, pipeline);

      //TODO: was context.resetToDefaults();
      if (msg.isResetConnection()) {
        logger.debug("Received reset connection flag during login");
      }
    }

//    if (!loginVisitor.isLoginSuccessful()) {
//      throw new IllegalStateException("Login failed: " + loginVisitor.getErrorMessage());
//    }

    logger.debug("Login7 phase completed successfully");
    return loginVisitor;
  }
}