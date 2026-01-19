package org.tdslib.javatdslib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.packets.TdsMessage;
import org.tdslib.javatdslib.packets.PacketType;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Flow;

/**
 * High-level TDS client facade.
 * Provides a simple connect + execute interface, hiding protocol details.
 */
public class TdsClient implements ConnectionContext, AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TdsClient.class);

  private final TdsTransport transport;
  private final TokenDispatcher tokenDispatcher;

  private boolean connected = false;

  private String currentDatabase = null;
  private String currentLanguage = "us_english"; // Default
  private String currentCharset = null; // Legacy, usually null
  private int packetSize = 4096;
  private byte[] currentCollationBytes = new byte[0];
  private boolean inTransaction = false;
  private String serverName = null;
  private String serverVersionString = null;
  private int spid;

  private TdsVersion tdsVersion = TdsVersion.V7_4; // default
  private QueryResponseTokenVisitor currentPublisher;

  /**
   * Create a new TdsClient backed by a TCP transport to the given host/port.
   *
   * @param host remote host name or IP
   * @param port remote TCP port
   * @throws IOException if the underlying transport cannot be created
   */
  public TdsClient(String host, int port) throws IOException {
    this.transport = new TdsTransport(host, port, this::onMessagesReceived, this::errorHandler);
    this.tokenDispatcher = new TokenDispatcher();
    this.connected = false;
  }

  private void errorHandler(Throwable throwable) {
    if (currentPublisher != null) {
      currentPublisher.getSubscriber().onError(throwable);
    }
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
  private void preLoginInternal() throws IOException {
    TdsMessage msg = TdsMessage.createRequest(
        PacketType.PRE_LOGIN.getValue(),
        buildPreLoginPayload(false, false)
    );

    transport.sendMessageDirect(msg);

    List<TdsMessage> responses = transport.receiveFullResponse();

    PreLoginResponse preLoginResponse = processPreLoginResponse(responses);

    // Apply negotiated packet size
    int negotiatedSize = preLoginResponse.getNegotiatedPacketSize();
    if (negotiatedSize > 0) {
      packetSize = negotiatedSize;
      transport.setPacketSize(packetSize);
    }

    if (preLoginResponse.requiresEncryption()) {
      enableTls();
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
   * @param encryptIfNeeded whether to request encryption
   * @param supportMars     whether to advertise MARS support
   * @return payload buffer
   */
  private ByteBuffer buildPreLoginPayload(boolean encryptIfNeeded, boolean supportMars) {
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
    LoginResponse loginResponse = new LoginResponse(this);
    QueryContext queryContext = new QueryContext();

    for (TdsMessage msg : packets) {
      setSpid(msg.getSpid());
      // Dispatch tokens to the visitor (which handles ENVCHANGE, LOGINACK, errors, etc.)
      tokenDispatcher.processMessage(msg, this, queryContext, loginResponse);

      // Still handle reset flag separately (visitor doesn't cover message-level flags)
      if (msg.isResetConnection()) {
        resetToDefaults();
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
  public Flow.Publisher<RowWithMetadata> queryAsync(String sql) throws IOException {
    if (!connected) {
      throw new IllegalStateException("Not connected or not in async mode");
    }

    // Build SQL_BATCH payload: UTF-16LE string + NULL terminator (no length prefix)
    byte[] sqlBytes = (sql).getBytes(StandardCharsets.UTF_16LE);

    byte[] allHeaders = AllHeaders.forAutoCommit(1).toBytes();

    ByteBuffer payload = ByteBuffer.allocate(allHeaders.length + sqlBytes.length);
    payload.put(allHeaders);
    payload.put(sqlBytes);

    payload.flip();

    // Create SQL_BATCH message
    TdsMessage queryMsg = TdsMessage.createRequest(PacketType.SQL_BATCH.getValue(), payload);

    // 2. Instead of blocking send/receive:
    //    → queue the message, register OP_WRITE if needed
    //    → return future that will be completed from selector loop

    currentPublisher = queryReactive(queryMsg);

    return currentPublisher;
  }

  private QueryResponseTokenVisitor queryReactive(TdsMessage queryMsg) {

    return new QueryResponseTokenVisitor(this, transport, queryMsg);
  }

  /**
   * Invoked by the transport when a TDS tdsMessage arrives.
   *
   * <p>This method dispatches tokens contained in the provided tdsMessage to the
   * currently active token visitor (via {@link TokenDispatcher}). The visitor
   * is responsible for handling token-level semantics (ENVCHANGE, LOGINACK,
   * row publishing, errors, etc.). After dispatching, if the tdsMessage signals
   * a connection-level reset (resetConnection flag) the client's session state
   * is reset to library defaults.
   *
   * @param tdsMessage the received {@link TdsMessage}; expected to be non-null and
   *                containing tokens to process
   */
  public void onMessagesReceived(TdsMessage tdsMessage) {

    // Dispatch tokens to the visitor (which handles ENVCHANGE, errors, etc.)
    tokenDispatcher.processMessage(tdsMessage, this, new QueryContext(), currentPublisher);

    // Still handle reset flag separately (visitor doesn't cover tdsMessage-level flags)
    if (tdsMessage.isResetConnection()) {
      resetToDefaults();
    }

  }

  /**
   * Enable TLS on the transport (placeholder).
   *
   * @throws IOException when TLS setup fails or unsupported
   */
  private void enableTls() throws IOException {
    // Implement TLS handshake
    throw new UnsupportedOperationException("TLS not yet implemented");
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

  @Override
  public TdsVersion getTdsVersion() {
    return tdsVersion;
  }

  @Override
  public void setTdsVersion(TdsVersion version) {
    this.tdsVersion = version;
  }

  @Override
  public boolean isUnicodeEnabled() {
    return tdsVersion.ordinal() >= TdsVersion.V7_1.ordinal(); // Unicode from TDS 7.1+
  }

  @Override
  public String getCurrentDatabase() {
    return currentDatabase;
  }

  @Override
  public int getCurrentPacketSize() {
    return packetSize;
  }

  @Override
  public void setDatabase(String database) {
    this.currentDatabase = database;
  }

  @Override
  public String getCurrentLanguage() {
    return currentLanguage;
  }

  @Override
  public void setLanguage(String language) {
    this.currentLanguage = language;
  }

  @Override
  public String getCurrentCharset() {
    return currentCharset;
  }

  @Override
  public void setCharset(String charset) {
    this.currentCharset = charset;
  }

  /**
   * Set the client packet size and propagate the value to the transport.
   *
   * @param size packet size in bytes
   */
  @Override
  public void setPacketSize(int size) {
    this.packetSize = size;
    transport.setPacketSize(size);
  }

  @Override
  public byte[] getCurrentCollationBytes() {
    return Arrays.copyOf(currentCollationBytes, currentCollationBytes.length); // Defensive copy
  }

  @Override
  public void setCollationBytes(byte[] collationBytes) {
    this.currentCollationBytes = collationBytes != null
        ? Arrays.copyOf(collationBytes, collationBytes.length)
        : new byte[0];
  }

  @Override
  public boolean isInTransaction() {
    return inTransaction;
  }

  @Override
  public void setInTransaction(boolean inTransaction) {
    this.inTransaction = inTransaction;
  }

  @Override
  public String getServerName() {
    return serverName;
  }

  @Override
  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  @Override
  public String getServerVersionString() {
    return serverVersionString;
  }

  @Override
  public void setServerVersionString(String versionString) {
    this.serverVersionString = versionString;
  }

  public int getSpid() {
    return spid;
  }

  public void setSpid(int spid) {
    this.spid = spid;
  }

  /**
   * Reset session-scoped state to library defaults.
   *
   * <p>This is called when the server signals a connection-level reset
   * (resetConnection flag) to clear per-session settings while preserving
   * connection-level information such as TDS version and server name.
   */
  @Override
  public void resetToDefaults() {
    currentDatabase = null;
    currentLanguage = "us_english";
    currentCharset = null;
    packetSize = 4096;
    currentCollationBytes = new byte[0];
    inTransaction = false;
    spid = 0;
    // Do NOT reset tdsVersion, serverName, or serverVersionString (connection-level)
    System.out.println("Session state reset due to resetConnection flag");
  }

}
