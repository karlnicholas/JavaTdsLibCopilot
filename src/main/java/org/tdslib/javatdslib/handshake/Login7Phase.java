package org.tdslib.javatdslib.handshake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.LoginResponse;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
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

import java.util.List;

public class Login7Phase {
  private static final Logger logger = LoggerFactory.getLogger(Login7Phase.class);

  public LoginVisitor execute(TdsTransport transport, ConnectionContext context,
                               String hostname, String username, String password, String database) throws Exception {
    logger.debug("Starting Login7 phase");

    Login7Options l7Opts = new Login7Options();
    Login7Payload login7Payload = new Login7Payload(l7Opts);
    login7Payload.hostname = hostname;
    login7Payload.database = database;
    login7Payload.username = username;
    login7Payload.password = password;

    TdsMessage login7Msg = TdsMessage.createRequest(PacketType.LOGIN7.getValue(), login7Payload.buildBuffer());

    if (transport.isTlsActive()) {
      transport.sendMessageEncrypted(login7Msg);
    } else {
      transport.sendMessageDirect(login7Msg);
    }

    List<TdsMessage> loginResponseMsgs = transport.receiveFullResponse();
    return processLoginResponse(transport, context, loginResponseMsgs);
  }

  private LoginVisitor processLoginResponse(TdsTransport transport, ConnectionContext context, List<TdsMessage> packets) {
    org.tdslib.javatdslib.tokens.visitors.LoginVisitor loginVisitor = new LoginVisitor();

    // Compose the Pipeline for Authentication
    CompositeTokenVisitor pipeline = new CompositeTokenVisitor(
        new EnvChangeVisitor(context),
        new MessageVisitor(null), // Terminal streams not needed during sync login
        loginVisitor
    );

    QueryContext queryContext = new QueryContext();
    TokenDispatcher tokenDispatcher = new TokenDispatcher(TokenParserRegistry.DEFAULT);

    for (TdsMessage msg : packets) {
      context.setSpid(msg.getSpid());
      tokenDispatcher.processMessage(msg, context, queryContext, pipeline);

      if (msg.isResetConnection()) {
        context.resetToDefaults();
      }
    }

    return loginVisitor;
  }
}