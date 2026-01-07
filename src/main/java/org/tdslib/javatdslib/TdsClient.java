// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib;

import org.tdslib.javatdslib.messages.MessageHandler;
import org.tdslib.javatdslib.messages.Message;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.payloads.login7.Login7Options;
import org.tdslib.javatdslib.payloads.login7.Login7Payload;
import org.tdslib.javatdslib.payloads.prelogin.PreLoginPayload;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.transport.TcpTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * High-level TDS client facade.
 * Provides a simple connect + execute interface, hiding protocol details.
 */
public class TdsClient implements ConnectionContext, AutoCloseable {

    private final MessageHandler messageHandler;
    private final TcpTransport transport;
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

    private TdsVersion tdsVersion = TdsVersion.V7_4; // default

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
        this.currentCollationBytes = collationBytes != null ? Arrays.copyOf(collationBytes, collationBytes.length) : new byte[0];
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

    @Override
    public void resetToDefaults() {
        currentDatabase = null;
        currentLanguage = "us_english";
        currentCharset = null;
        packetSize = 4096;
        currentCollationBytes = new byte[0];
        inTransaction = false;
        // Do NOT reset tdsVersion, serverName, or serverVersionString (connection-level)
        System.out.println("Session state reset due to resetConnection flag");
    }

    public TdsClient(String host, int port) throws IOException {
        this.transport = new TcpTransport(host, port);
        this.messageHandler = new MessageHandler(transport);
        this.tokenDispatcher = new TokenDispatcher();
        this.connected = false;
    }

    public void connect(String username, String password, String database, String appName) throws IOException {
        if (connected) {
            throw new IllegalStateException("Already connected");
        }

        preLoginInternal();
        transport.enableTls();
        loginInternal(username, password, database, appName);

        connected = true;
    }

    private void preLoginInternal() throws IOException {
        Message msg = Message.createRequest(PacketType.PRE_LOGIN.getValue(), buildPreLoginPayload(false, false));

        messageHandler.sendMessage(msg);

        List<Message> responses = messageHandler.receiveFullResponse();

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

    private void loginInternal(String username, String password, String database, String appName) throws IOException {
        ByteBuffer loginPayload = buildLogin7Payload(username, password, database, appName);

        Message loginMsg = new Message(
                (byte) 0x10, (byte) 0x01, loginPayload.capacity() + 8,
                (short) 0, (short) 1, loginPayload, System.nanoTime(), null
        );

        messageHandler.sendMessage(loginMsg);

        List<Message> responses = messageHandler.receiveFullResponse();

        LoginResponse loginResponse = processLoginResponse(responses);

        if (!loginResponse.isSuccess()) {
            throw new IOException("Login failed: " + loginResponse.getErrorMessage());
        }

        currentDatabase = loginResponse.getDatabase();
    }

    private ByteBuffer buildPreLoginPayload(boolean encryptIfNeeded, boolean supportMars) {
        // Implement your PreLogin payload builder here
        // For now, placeholder
        PreLoginPayload preLoginPayload = new PreLoginPayload(false);
        return preLoginPayload.buildBuffer();

//        ByteBuffer payload = ByteBuffer.allocate(100).order(ByteOrder.LITTLE_ENDIAN);
//        payload.flip();
//        return payload;
    }

    private ByteBuffer buildLogin7Payload(String username, String password, String database, String appName) {
        // Implement your Login7 payload builder here
        Login7Payload login7Payload = new Login7Payload(new Login7Options());
        login7Payload.hostname = "192.168.1.121";
        login7Payload.serverName = "MyServerName";
        login7Payload.appName = "MyAppName";
        login7Payload.language = "us_english";
        login7Payload.database = "master";
        login7Payload.username = "reactnonreact";
        login7Payload.password = "reactnonreact";

//        login7Payload..TypeFlags.AccessIntent = TypeFlags.OptionAccessIntent.READ_WRITE;
        return login7Payload.buildBuffer();

//        Message login7Message = Message.createRequest(PacketType.LOGIN7.getValue(), );
//
//        payload.flip();
//        return payload;
    }

    private PreLoginResponse processPreLoginResponse(List<Message> packets) {
        ByteBuffer combined = combinePayloads(packets);
        PreLoginResponse response = new PreLoginResponse();

        // PreLogin is NOT token-based — it's a fixed option table
        // Parse manually (simple loop over option list)
        if (combined.hasRemaining()) {
            while (combined.hasRemaining()) {
                byte option = combined.get();
                if (option == (byte) 0xFF) break; // terminator

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
                    case 0x04: // PACKETSIZE (optional in PreLogin, but some servers send it)
                        // Read as B_VARCHAR (length byte + string)
                        int psLen = combined.get() & 0xFF;
                        byte[] psBytes = new byte[psLen];
                        combined.get(psBytes);
                        String psStr = new String(psBytes, StandardCharsets.US_ASCII);
                        try {
                            response.setNegotiatedPacketSize(Integer.parseInt(psStr));
                        } catch (NumberFormatException ignored) {}
                        break;
                    // Add MARS, INSTANCE, etc. as needed
                }

                combined.position(savedPos);
            }
        }

        return response;
    }

    private LoginResponse processLoginResponse(List<Message> packets) {
        LoginResponse loginResponse = new LoginResponse(this);

        // Create the visitor once — it will apply changes to 'this' (TdsClient as ConnectionContext)
//        ApplyingTokenVisitor visitor = new ApplyingTokenVisitor(this);

        for (Message msg : packets) {
            // Dispatch tokens to the visitor (which handles applyEnvChange, login ack, errors, etc.)
            tokenDispatcher.processMessage(msg, this, loginResponse);

            // Still handle reset flag separately (visitor doesn't cover message-level flags)
            if (msg.isResetConnection()) {
                resetToDefaults();
            }
        }

        // Optional: After full login response, check if we have a successful LoginAck
        // (you can add a flag or check in visitor if needed)
        return loginResponse;
    }

    private void enableTls() throws IOException {
        // Implement TLS handshake
        throw new UnsupportedOperationException("TLS not yet implemented");
    }

    private ByteBuffer combinePayloads(List<Message> packets) {
        int total = packets.stream().mapToInt(m -> m.getPayload().remaining()).sum();
        ByteBuffer combined = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
        for (Message m : packets) {
            combined.put(m.getPayload().duplicate());
        }
        combined.flip();
        return combined;
    }

    public void close() throws IOException {
        messageHandler.close();
        connected = false;
    }

    public boolean isConnected() {
        return connected;
    }
}