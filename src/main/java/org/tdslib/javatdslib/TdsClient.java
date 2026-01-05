// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib;

import org.tdslib.javatdslib.io.MessageHandler;
import org.tdslib.javatdslib.messages.Message;
import org.tdslib.javatdslib.packets.PacketType;
import org.tdslib.javatdslib.payloads.login7.Login7Options;
import org.tdslib.javatdslib.payloads.login7.Login7Payload;
import org.tdslib.javatdslib.payloads.prelogin.PreLoginPayload;
import org.tdslib.javatdslib.tokens.TokenDispatcher;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeType;
import org.tdslib.javatdslib.tokens.error.ErrorToken;
import org.tdslib.javatdslib.tokens.loginack.LoginAckToken;
import org.tdslib.javatdslib.transport.TcpTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.tdslib.javatdslib.tokens.envchange.EnvChangeType.PACKET_SIZE;
import static org.tdslib.javatdslib.tokens.envchange.EnvChangeType.PACKET_SIZE_ALT;

/**
 * High-level TDS client facade.
 * Provides a simple connect + execute interface, hiding protocol details.
 */
public class TdsClient implements ConnectionContext {

    private final MessageHandler messageHandler;
    private final TcpTransport transport;
    private final TokenDispatcher tokenDispatcher;

    private boolean connected = false;

    private String currentDatabase = null;
    private int packetSize = 4096;

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
    public void setPacketSize(int size) {
        this.packetSize = size;
        transport.setPacketSize(size);
    }

    @Override
    public void resetToDefaults() {
        currentDatabase = null;
        packetSize = 4096;
        // Reset other state as needed
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
        loginInternal(username, password, database, appName);

        connected = true;
    }

    private void preLoginInternal() throws IOException {
        Message msg = Message.createRequest(PacketType.PRE_LOGIN.getValue(), buildPreLoginPayload(true, false));

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
            combined.get(); // skip first byte (usually 0x00 or option count)
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
        LoginResponse loginResponse = new LoginResponse();

        // Use TokenDispatcher to process each packet individually
        for (Message msg : packets) {
            tokenDispatcher.processMessage(msg, this, token -> {
                if (token instanceof LoginAckToken ack) {
                    loginResponse.setSuccess(true);
                } else if (token instanceof ErrorToken err) {
                    loginResponse.setSuccess(false);
                    loginResponse.setErrorMessage(err.getMessage());
                } else if (token instanceof EnvChangeToken change) {
                    loginResponse.addEnvChange(change);

                    // Apply immediately — clean & extensible
                    switch (change.getChangeType()) {
                        case PACKET_SIZE, PACKET_SIZE_ALT -> setPacketSize(change.getNewValueAsInt());
                        case DATABASE -> setDatabase(change.getNewValue());
                        case LANGUAGE -> { /* future: setLanguage(...) */ }
                        case UNKNOWN -> { /* log warning */ }
                    }
                }
                // Handle INFO, DONE, etc. as needed
            });

            // Handle resetConnection flag if present
            if (msg.isResetConnection()) {
                resetToDefaults();
            }
        }

        return loginResponse;
    }

    private void enableTls() throws IOException {
        // Implement TLS handshake
        throw new UnsupportedOperationException("TLS not yet implemented");
    }

    private ByteBuffer combinePayloads(List<Message> packets) {
        int total = packets.stream().mapToInt(m -> m.getPayload().remaining()).sum();
        ByteBuffer combined = ByteBuffer.allocate(total).order(ByteOrder.LITTLE_ENDIAN);
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