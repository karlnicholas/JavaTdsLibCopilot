// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens;

import org.tdslib.javatdslib.TdsClient;
import org.tdslib.javatdslib.io.connection.ConnectionOptions;
import org.tdslib.javatdslib.messages.Message;
import org.tdslib.javatdslib.messages.MessageHandler;
import org.tdslib.javatdslib.tokens.done.DoneInProcTokenParser;
import org.tdslib.javatdslib.tokens.done.DoneProcTokenParser;
import org.tdslib.javatdslib.tokens.done.DoneTokenParser;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeTokenParser;
import org.tdslib.javatdslib.tokens.error.ErrorTokenParser;
import org.tdslib.javatdslib.tokens.featureextack.FeatureExtAckTokenParser;
import org.tdslib.javatdslib.tokens.fedauth.FedAuthInfoTokenParser;
import org.tdslib.javatdslib.tokens.info.InfoTokenParser;
import org.tdslib.javatdslib.tokens.loginack.LoginAckTokenParser;
import org.tdslib.javatdslib.tokens.returnstatus.ReturnStatusTokenParser;
import org.tdslib.javatdslib.tokens.returnvalue.ReturnValueTokenParser;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Token stream handler.
 */
public class TokenStreamHandler {
    private final TdsClient client;
    private final Map<TokenType, TokenParser> parsers;
    private ByteBuffer incomingTokenBuffer;

    public TokenStreamHandler(TdsClient client) {
        this.client = client;
        this.parsers = new HashMap<>();
        // TODO: Ensure TokenParser interface is now synchronous: Token parse(TokenType type, TokenStreamHandler handler)
        parsers.put(TokenType.ERROR, new ErrorTokenParser());
        parsers.put(TokenType.INFO, new InfoTokenParser());
        parsers.put(TokenType.DONE, new DoneTokenParser());
        parsers.put(TokenType.DONE_PROC, new DoneProcTokenParser());
        parsers.put(TokenType.DONE_IN_PROC, new DoneInProcTokenParser());
        parsers.put(TokenType.LOGIN_ACK, new LoginAckTokenParser());
        parsers.put(TokenType.ENV_CHANGE, new EnvChangeTokenParser());
        parsers.put(TokenType.RETURN_STATUS, new ReturnStatusTokenParser());
        parsers.put(TokenType.RETURN_VALUE, new ReturnValueTokenParser());
        parsers.put(TokenType.FEATURE_EXT_ACK, new FeatureExtAckTokenParser());
        parsers.put(TokenType.FED_AUTH_INFO, new FedAuthInfoTokenParser());
    }

    /**
     * Connection options.
     */
    public ConnectionOptions getOptions() {
        return client.getConnection().getOptions();
    }

    /**
     * Receive a token.
     */
    public Token receiveToken() {
        int type = readUInt8();
        TokenType tokenType = TokenType.fromValue((byte) type);

        if (tokenType == null) {
            throw new IllegalArgumentException("Unsupported Token type: 0x" + Integer.toHexString(type));
        }
        if (!parsers.containsKey(tokenType)) {
            throw new IllegalArgumentException("Unsupported Token type: " + tokenType);
        }

        // Assumes TokenParser.parse is now synchronous
        return parsers.get(tokenType).parse(tokenType, this);
    }

    /**
     * Receives tokens until the end of data or the receiver exits.
     */
    public void receiveTokens(Consumer<TokenEvent> tokenReceiver) {
        TokenEvent tokenEvent = new TokenEvent();
        try {
            // We loop as long as we can fetch data.
            // dataAvailable checks if current buffer has data, if not it tries to fetch more.
            while (ensureDataAvailable()) {
                Token token = receiveToken();
                tokenEvent.setToken(token);
                tokenReceiver.accept(tokenEvent);
                if (tokenEvent.isExit()) {
                    break;
                }
            }
        } finally {
            clearBuffer();
        }
    }

    /**
     * Ensures that the buffer has at least 'size' bytes available.
     * If not, it blocks and fetches more messages from the server until it does.
     */
    private void ensureData(int size) {
        if (incomingTokenBuffer == null) {
            incomingTokenBuffer = ByteBuffer.allocate(0);
        }

        while (incomingTokenBuffer.remaining() < size) {
            MessageHandler messageHandler = client.getMessageHandler();

            // This is a blocking call
            Message message = messageHandler.receiveMessage();
            ByteBuffer newChunk = message.getPayload().getBuffer();

            // Optimization: If old buffer is empty, just swap.
            if (!incomingTokenBuffer.hasRemaining()) {
                incomingTokenBuffer = newChunk;
            } else {
                // Otherwise, concatenate.
                ByteBuffer expanded = ByteBuffer.allocate(incomingTokenBuffer.remaining() + newChunk.remaining());
                expanded.put(incomingTokenBuffer);
                expanded.put(newChunk);
                expanded.flip();
                incomingTokenBuffer = expanded;
            }
        }
    }

    /**
     * Checks if data is available or can be fetched.
     * Used by the loop to know if it should continue trying to read tokens.
     */
    private boolean ensureDataAvailable() {
        if (incomingTokenBuffer != null && incomingTokenBuffer.hasRemaining()) {
            return true;
        }
        // Try to fetch at least 1 byte to see if stream is alive
        try {
            ensureData(1);
            return true;
        } catch (Exception e) {
            // If fetching failed (e.g. socket closed), return false to stop loop
            return false;
        }
    }

    private void clearBuffer() {
        incomingTokenBuffer = null;
    }

    // --- Read Methods (Synchronous) ---

    public int readUInt8() {
        ensureData(1);
        return incomingTokenBuffer.get() & 0xFF;
    }

    public int readUInt16LE() {
        ensureData(2);
        // ByteBuffer is usually Big Endian by default, but we can do manual bitwise for explicit LE control
        int b1 = incomingTokenBuffer.get() & 0xFF;
        int b2 = incomingTokenBuffer.get() & 0xFF;
        return b1 | (b2 << 8);
    }

    public long readUInt32LE() {
        ensureData(4);
        long b1 = incomingTokenBuffer.get() & 0xFF;
        long b2 = incomingTokenBuffer.get() & 0xFF;
        long b3 = incomingTokenBuffer.get() & 0xFF;
        long b4 = incomingTokenBuffer.get() & 0xFF;
        return b1 | (b2 << 8) | (b3 << 16) | (b4 << 24);
    }

    public long readUInt32BE() {
        ensureData(4);
        long b1 = incomingTokenBuffer.get() & 0xFF;
        long b2 = incomingTokenBuffer.get() & 0xFF;
        long b3 = incomingTokenBuffer.get() & 0xFF;
        long b4 = incomingTokenBuffer.get() & 0xFF;
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    public long readUInt64LE() {
        ensureData(8);
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result |= ((long) (incomingTokenBuffer.get() & 0xFF)) << (i * 8);
        }
        return result;
    }

    public String readBVarChar() {
        int length = readUInt8(); // 1 byte length
        if (length == 0) {
            return "";
        }
        return readString(length * 2);
    }

    public String readUsVarChar() {
        int length = readUInt16LE(); // 2 byte length
        if (length == 0) {
            return "";
        }
        // length in TDS UsVarChar is usually in CHARACTERS, but for standard string reads we might need bytes.
        // Usually, TDS assumes UCS-2 (2 bytes per char).
        return readString(length * 2);
    }

    private String readString(int byteLength) {
        ensureData(byteLength);
        byte[] bytes = new byte[byteLength];
        incomingTokenBuffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_16LE);
    }

    public byte[] readBytes(int length) {
        if (length == 0) {
            return new byte[0];
        }
        ensureData(length);
        byte[] bytes = new byte[length];
        incomingTokenBuffer.get(bytes);
        return bytes;
    }
}