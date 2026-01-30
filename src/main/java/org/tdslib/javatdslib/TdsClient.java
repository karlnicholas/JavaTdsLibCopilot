package org.tdslib.javatdslib;

import io.r2dbc.spi.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.headers.AllHeaders;
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
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * High-level TDS client facade.
 * Provides a simple connect + execute interface, hiding protocol details.
 */
public class TdsClient implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TdsClient.class);

  private final TdsTransport transport;
  private boolean connected;

  /**
   * Create a new TdsClient backed by a TCP transport to the given host/port.
   *
   * @param host remote host name or IP
   * @param port remote TCP port
   * @throws IOException if the underlying transport cannot be created
   */
  public TdsClient(String host, int port) throws IOException {
    this.transport = new TdsTransport(host, port);
    this.connected = false;
  }

  /**
   * Connect to the server and perform prelogin + login.
   *
   * @param hostname   server host name for LOGIN payload
   * @param username   login user
   * @param password   login password
   * @param database   initial database
   * @param appName    application name
   * @param serverName client-reported server name
   * @param language   language name
   * @throws IOException on IO errors or failed login
   */
  public void connect(String hostname, String username, String password, String database,
                      String appName, String serverName, String language
  ) throws IOException, NoSuchAlgorithmException, KeyManagementException {
    if (connected) {
      throw new IllegalStateException("Already connected");
    }

    preLoginInternal();
    transport.tlsHandshake();
    LoginResponse loginResponse = loginInternal(
        hostname, username, password, database, appName, serverName, language
    );

    if (!loginResponse.isSuccess()) {
      throw new IOException("Login failed: " + loginResponse.getErrorMessage());
    }

    transport.tlsComplete();

    // Switch socket to non-blocking + register to selector
    transport.enterAsyncMode();

    connected = true;
  }

  /**
   * Perform the PreLogin exchange and apply negotiated options.
   *
   * @throws IOException on IO error
   */
  private void preLoginInternal()
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
  private LoginResponse loginInternal(String hostname, String username, String password,
                                      String database, String appName, String serverName,
                                      String language) throws IOException {
    ByteBuffer loginPayload = buildLogin7Payload(
        hostname, username, password, database, appName, serverName, language
    );

    TdsMessage loginMsg = TdsMessage.createRequest(PacketType.LOGIN7.getValue(), loginPayload);

    transport.sendMessageEncrypted(loginMsg);

    List<TdsMessage> responses = transport.receiveFullResponse();

    return processLoginResponse(responses);
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
                                        String database, String appName, String serverName,
                                        String language) {
    Login7Payload login7Payload = new Login7Payload(new Login7Options());
    login7Payload.hostname = hostname;
    login7Payload.serverName = serverName;
    login7Payload.appName = appName;
    login7Payload.language = language;
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
  private LoginResponse processLoginResponse(List<TdsMessage> packets) {
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
   * Execute a SQL query and return the high-level QueryResponse.
   *
   * @param sql SQL text to execute
   * @return QueryResponse containing results or errors
   * @throws IOException on I/O or transport errors
   */
  public Statement queryAsync(String sql) {
    return new TdsStatementImpl(sql, transport);
  }

  public Statement queryRpc(String sql) {
    return new TdsStatementImpl(sql, transport);
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


  /**
   * Close the client and release underlying resources.
   *
   * @throws IOException when closing the underlying message handler fails.
   */
  @Override
  public void close() throws IOException {
    logger.debug("Closing TdsClient");
    transport.close();
    connected = false;
  }

  public boolean isConnected() {
    return connected;
  }

  public SocketChannel getSocketChannel() {
    return transport.getSocketChannel();
  }

}
