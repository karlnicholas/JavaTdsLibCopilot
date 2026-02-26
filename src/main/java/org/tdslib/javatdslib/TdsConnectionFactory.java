package org.tdslib.javatdslib;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.payloads.login7.Login7Options;
import org.tdslib.javatdslib.payloads.login7.Login7Payload;
import org.tdslib.javatdslib.payloads.prelogin.PreLoginPayload;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.DefaultConnectionContext;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsConnectionFactory implements ConnectionFactory {
  private static final Logger logger = LoggerFactory.getLogger(TdsConnectionFactory.class);
  public static final Option<Integer> CONNECT_RETRIES = Option.valueOf("connectRetries");
  public static final Option<Long> CONNECT_RETRY_DELAY = Option.valueOf("connectRetryDelay");
  // ... other options

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

              // 1. Create the Context and pass it to Transport
              ConnectionContext context = new DefaultConnectionContext();
              TdsTransport transport = new TdsTransport(hostname, port, context);

              // Perform Handshake synchronously
              try {
                // 2. Pass context down to doHandshake
                doHandshake(transport, context, hostname, username, password, database);

                // Switch to Async mode for queries
                transport.enterAsyncMode();

                // 3. Pass both transport and context to TdsConnection
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

  // 4. Update signature to accept context
  private void doHandshake(TdsTransport transport, ConnectionContext context,
                           String hostname, String username, String password, String database) throws Exception {

    // --- 1. Pre-Login Phase ---
    logger.debug("Starting Pre-Login phase");
    PreLoginPayload preLoginPayload = new PreLoginPayload(false);

    // ... configuration logic for preLogin ...
    TdsMessage preLoginMsg = TdsMessage.createRequest(PacketType.PRE_LOGIN.getValue(), preLoginPayload.buildBuffer());
    transport.sendMessageDirect(preLoginMsg);

    List<TdsMessage> preLoginResponses = transport.receiveFullResponse();
    PreLoginResponse preLoginResponse = processPreLoginResponse(preLoginResponses);

    // 5. Use context instead of transport to set packet size
    context.setPacketSize(preLoginResponse.getNegotiatedPacketSize());

    // --- THE FIX IS HERE ---
    // 0x00 = ENCRYPT_OFF (Login-only TLS required)
    // 0x01 = ENCRYPT_REQ (Full TLS required)
    // 0x02 = ENCRYPT_NOT_SUP (No TLS supported)
    int serverEncryption = preLoginResponse.getEncryption();
    if (serverEncryption == 0x00 || serverEncryption == 0x01) {
      transport.tlsHandshake();
    }


    // --- 3. Login7 Phase ---
    logger.debug("Starting Login7 phase");
    Login7Options l7Opts = new Login7Options();
    // ... configuration logic for Login7 ...

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

    // 6. Pass context to processLoginResponse
    LoginResponse loginResult = processLoginResponse(transport, context, loginResponseMsgs);

    if (!loginResult.isSuccess()) {
      throw new R2dbcNonTransientResourceException(
          loginResult.getErrorMessage() != null ? loginResult.getErrorMessage() : "Login Failed"
      );
    }

    logger.debug("Login successful. Database: {}", loginResult.getDatabase());
    transport.tlsComplete();
  }

  /**
   * Parse PreLogin response messages and return a consolidated response.
   *
   * @param packets messages returned from server for prelogin
   * @return parsed PreLoginResponse
   */
  private PreLoginResponse processPreLoginResponse(List<TdsMessage> packets) {
    ByteBuffer combined = combinePayloads(packets);
    PreLoginResponse response = new PreLoginResponse();

    // PreLogin is NOT token-based — it's a fixed option table
    if (combined.hasRemaining()) {
      while (combined.hasRemaining()) {
        byte option = combined.get();
        if (option == (byte) 0xFF) {
          break; // terminator
        }

        short offset = combined.getShort();
        short length = combined.getShort();

        int savedPos = combined.position();
        combined.position(offset);

        switch (option) {
          case 0x00: // VERSION
            int major = combined.get() & 0xFF;
            int minor = combined.get() & 0xFF;
            short build = combined.getShort();
            response.setVersion(major, minor, build);
            break;

          case 0x01: // ENCRYPTION
            byte enc = combined.get();
            response.setEncryption(enc);
            break;

          case 0x04: // PACKETSIZE (optional in PreLogin)
            // Read as B_VARCHAR (length byte + string)
            int psLen = combined.get() & 0xFF;
            byte[] psBytes = new byte[psLen];
            combined.get(psBytes);
            String psStr = new String(psBytes, StandardCharsets.US_ASCII);
            try {
              response.setNegotiatedPacketSize(Integer.parseInt(psStr));
            } catch (NumberFormatException e) {
              logger.error("Failed to parse negotiated packet size: {}", psStr);
            }
            break;

          default:
            // Unknown prelogin option, ignore safely
            break;
        }

        combined.position(savedPos);
      }
    }

    return response;
  }

  // 7. Update signature to accept context
  private LoginResponse processLoginResponse(TdsTransport transport, ConnectionContext context, List<TdsMessage> packets) {
    // 8. Pass context to LoginResponse
    LoginResponse loginResponse = new LoginResponse(transport, context);
    QueryContext queryContext = new QueryContext();
    TokenDispatcher tokenDispatcher = new TokenDispatcher();

    for (TdsMessage msg : packets) {
      // 9. Use context instead of transport
      context.setSpid(msg.getSpid());

      // 10. Pass context to processMessage
      tokenDispatcher.processMessage(msg, context, queryContext, loginResponse);

      // Still handle reset flag separately (visitor doesn't cover message-level flags)
      if (msg.isResetConnection()) {
        // 11. Use context instead of transport
        context.resetToDefaults();
      }
    }

    return loginResponse;
  }

  private ByteBuffer combinePayloads(List<TdsMessage> packets) {
    int total = packets.stream()
        .mapToInt(m -> m.getPayload().remaining())
        .sum();

    ByteBuffer combined = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
    for (TdsMessage m : packets) {
      combined.put(m.getPayload().duplicate());
    }
    combined.flip();
    return combined;
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return null;
  }
}