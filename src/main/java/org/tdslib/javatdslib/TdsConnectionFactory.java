package org.tdslib.javatdslib;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.payloads.login7.Login7Options;
import org.tdslib.javatdslib.payloads.login7.Login7Payload;
import org.tdslib.javatdslib.payloads.prelogin.PreLoginPayload;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class TdsConnectionFactory implements ConnectionFactory {
  private final ConnectionFactoryOptions options;
  public TdsConnectionFactory(ConnectionFactoryOptions options) {
    this.options = options;
  }

  @Override
  public Publisher<? extends Connection> create() {
    return subscriber -> subscriber.onSubscribe(new Subscription() {

      // Flag to handle cancellation safely
      private volatile boolean cancelled = false;

      @Override
      public void request(long n) {
        if (cancelled) {
          return;
        }

        TdsTransport transport = null;
        try {
          // 1. Safety: Validate required options exist before attempting connection
          // 2. Extraction: Get values safely
          String hostname = (String) options.getValue(HOST);
          int port = (Integer) options.getValue(PORT);
          String username = (String) options.getValue(USER);
          String password = (String) options.getValue(PASSWORD);
          String database = (String) options.getValue(DATABASE);

          // 3. Connection: Initialize transport
          transport = new TdsTransport(hostname, port);

          // 4. Handshake: PreLogin and TLS
          preLoginInternal(transport);
          transport.tlsHandshake(); //

          // 5. Login: Perform authentication
          LoginResponse loginResponse = loginInternal(transport, hostname, username, password, database); //

          if (!loginResponse.isSuccess()) {
            throw new IOException("Login failed: " + loginResponse.getErrorMessage());
          }

          transport.tlsComplete(); //

          // 6. Async Switch: Switch socket to non-blocking + register to selector
          transport.enterAsyncMode(); //

          // 7. Emission: Emit the active connection
          subscriber.onNext(new TdsConnection(transport));
          subscriber.onComplete();

        } catch (Throwable t) {
          // 8. Cleanup: If ANY step fails, ensure the transport (socket) is closed
          if (transport != null) {
            try {
              transport.close();
            } catch (IOException closeEx) {
              t.addSuppressed(closeEx); // Attach close error to the original error
            }
          }
          // Signal the error downstream to the client
          subscriber.onError(t);
        }
      }

      @Override
      public void cancel() {
        this.cancelled = true;
      }

    });
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return TdsConnectionFactoryMetadataImpl.INSTANCE;
  }

  /**
   * Metadata implementation for the TDS Connection Factory.
   * Identifies the database product name.
   */
  static class TdsConnectionFactoryMetadataImpl implements ConnectionFactoryMetadata {

    static final TdsConnectionFactoryMetadataImpl INSTANCE = new TdsConnectionFactoryMetadataImpl();

    private TdsConnectionFactoryMetadataImpl() {
    }

    @Override
    public String getName() {
      // Returns the name of the database product this factory connects to
      return "JavaTdsLib MS SQL Server";
    }
  }

  /**
   * Perform the PreLogin exchange and apply negotiated options.
   *
   * @throws IOException on IO error
   */
  private void preLoginInternal(TdsTransport transport)
          throws IOException, NoSuchAlgorithmException, KeyManagementException {
    TdsMessage msg = TdsMessage.createRequest(
            PacketType.PRE_LOGIN.getValue(),
            buildPreLoginPayload()
    );

    transport.sendMessageDirect(msg);

    List<TdsMessage> responses = transport.receiveFullResponse();

    PreLoginResponse preLoginResponse = processPreLoginResponse(responses);

    // Apply negotiated packet size
    int negotiatedSize = preLoginResponse.getNegotiatedPacketSize();
    if (negotiatedSize > 0) {
      transport.setPacketSize(negotiatedSize);
    }

    if (preLoginResponse.requiresEncryption()) {
      transport.tlsHandshake();
    }
  }

  /**
   * Perform LOGIN7 exchange and return parsed result.
   *
   * @throws IOException on IO error
   */
  private LoginResponse loginInternal(TdsTransport transport, String hostname, String username, String password,
                                      String database) throws IOException {
    ByteBuffer loginPayload = buildLogin7Payload(hostname, username, password, database);

    TdsMessage loginMsg = TdsMessage.createRequest(PacketType.LOGIN7.getValue(), loginPayload);

    transport.sendMessageEncrypted(loginMsg);

    List<TdsMessage> responses = transport.receiveFullResponse();

    return processLoginResponse(transport, responses);
  }

  /**
   * Build PreLogin payload. Placeholder builder using PreLoginPayload helper.
   *
   * @return payload buffer
   */
  private ByteBuffer buildPreLoginPayload() {
    PreLoginPayload preLoginPayload = new PreLoginPayload(false);
    return preLoginPayload.buildBuffer();
  }

  /**
   * Build LOGIN7 payload from provided fields.
   *
   * @return built login payload buffer
   */
  private ByteBuffer buildLogin7Payload(String hostname, String username, String password,
                                        String database) {
    Login7Payload login7Payload = new Login7Payload(new Login7Options());
    login7Payload.hostname = hostname;
    login7Payload.database = database;
    login7Payload.username = username;
    login7Payload.password = password;

    return login7Payload.buildBuffer();
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
              // Log parsing failure so catch is not empty
              System.err.println("Failed to parse negotiated packet size: " + psStr);
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

  /**
   * Process the server's Login response messages and produce a LoginResponse.
   *
   * @param packets received messages that contain login-related tokens
   * @return populated LoginResponse reflecting login tokens and errors
   */
  private LoginResponse processLoginResponse(TdsTransport transport, List<TdsMessage> packets) {
    LoginResponse loginResponse = new LoginResponse(transport);
    QueryContext queryContext = new QueryContext();
    TokenDispatcher tokenDispatcher = new TokenDispatcher();

    for (TdsMessage msg : packets) {
      transport.setSpid(msg.getSpid());
      // Dispatch tokens to the visitor (which handles ENVCHANGE, LOGINACK, errors, etc.)
      tokenDispatcher.processMessage(msg, transport, queryContext, loginResponse);

      // Still handle reset flag separately (visitor doesn't cover message-level flags)
      if (msg.isResetConnection()) {
        transport.resetToDefaults();
      }
    }

    // Optional: After full login response, check if we have a successful LoginAck
    return loginResponse;
  }

  /**
   * Combine payload buffers from a list of messages into a single
   * big\-endian ByteBuffer containing the concatenated payload bytes.
   *
   * @param packets messages whose payloads should be merged
   * @return combined ByteBuffer ready for reading
   */
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

}
