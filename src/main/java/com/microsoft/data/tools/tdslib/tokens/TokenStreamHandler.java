// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens;

import com.microsoft.data.tools.tdslib.TdsClient;
import com.microsoft.data.tools.tdslib.io.connection.ConnectionOptions;
import com.microsoft.data.tools.tdslib.messages.Message;
import com.microsoft.data.tools.tdslib.messages.MessageHandler;
import com.microsoft.data.tools.tdslib.tokens.done.DoneTokenParser;
import com.microsoft.data.tools.tdslib.tokens.envchange.EnvChangeTokenParser;
import com.microsoft.data.tools.tdslib.tokens.error.ErrorTokenParser;
import com.microsoft.data.tools.tdslib.tokens.info.InfoTokenParser;
import com.microsoft.data.tools.tdslib.tokens.loginack.LoginAckTokenParser;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Token stream handler.
 */
public class TokenStreamHandler {
    private final TdsClient client;
    private final Map<TokenType, TokenParser> parsers;
    private ByteBuffer incomingTokenBuffer;
    private int offset;

    public TokenStreamHandler(TdsClient client) {
        this.client = client;
        this.parsers = new HashMap<>();
        // TODO: Add all parsers
        parsers.put(TokenType.ERROR, new ErrorTokenParser());
        parsers.put(TokenType.INFO, new InfoTokenParser());
        parsers.put(TokenType.DONE, new DoneTokenParser());
        parsers.put(TokenType.LOGIN_ACK, new LoginAckTokenParser());
        parsers.put(TokenType.ENV_CHANGE, new EnvChangeTokenParser());
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
    public CompletableFuture<Token> receiveTokenAsync() {
        return readUInt8()
            .thenCompose(type -> {
                TokenType tokenType = TokenType.fromValue((byte) (int) type);
                if (tokenType == null) {
                    throw new IllegalArgumentException("Unsupported Token type: 0x" + Integer.toHexString(type));
                }
                if (!parsers.containsKey(tokenType)) {
                    throw new IllegalArgumentException("Unsupported Token type: " + tokenType);
                }
                return parsers.get(tokenType).parse(tokenType, this)
                    .thenApply(token -> {
                        trimBuffer();
                        return token;
                    });
            });
    }

    /**
     * Receives tokens until the end of data or the receiver exits.
     */
    public CompletableFuture<Void> receiveTokensAsync(Consumer<TokenEvent> tokenReceiver) {
        TokenEvent tokenEvent = new TokenEvent();
        return CompletableFuture.runAsync(() -> {
            try {
                while (dataAvailable()) {
                    Token token = receiveTokenAsync().join();
                    tokenEvent.setToken(token);
                    tokenReceiver.accept(tokenEvent);
                    if (tokenEvent.isExit()) {
                        break;
                    }
                }
            } finally {
                clearBuffer();
            }
        });
    }

    private CompletableFuture<Void> waitForData(int size) {
        if (incomingTokenBuffer == null) {
            MessageHandler messageHandler = client.getMessageHandler();
            return messageHandler.receiveMessage()
                .thenAccept(message -> {
                    incomingTokenBuffer = message.getPayload().getBuffer();
                })
                .thenCompose(v -> {
                    if (incomingTokenBuffer.remaining() < offset + size) {
                        return waitForData(size);
                    }
                    return CompletableFuture.completedFuture(null);
                });
        } else {
            if (incomingTokenBuffer.remaining() < offset + size) {
                MessageHandler messageHandler = client.getMessageHandler();
                return messageHandler.receiveMessage()
                    .thenAccept(message -> {
                        // Concat buffers
                        ByteBuffer newBuffer = ByteBuffer.allocate(incomingTokenBuffer.remaining() + message.getPayload().getBuffer().remaining());
                        newBuffer.put(incomingTokenBuffer);
                        newBuffer.put(message.getPayload().getBuffer());
                        newBuffer.flip();
                        incomingTokenBuffer = newBuffer;
                    })
                    .thenCompose(v -> waitForData(size));
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private boolean dataAvailable() {
        return incomingTokenBuffer != null && incomingTokenBuffer.remaining() > offset;
    }

    private void trimBuffer() {
        if (incomingTokenBuffer == null) {
            return;
        }
        if (offset == incomingTokenBuffer.remaining()) {
            incomingTokenBuffer = null;
        } else {
            incomingTokenBuffer.position(incomingTokenBuffer.position() + offset);
            incomingTokenBuffer = incomingTokenBuffer.slice();
        }
        offset = 0;
    }

    private void clearBuffer() {
        incomingTokenBuffer = null;
        offset = 0;
    }

    // Read methods
    public CompletableFuture<Integer> readUInt8() {
        return waitForData(1)
            .thenApply(v -> {
                int value = incomingTokenBuffer.get() & 0xFF;
                offset += 1;
                return value;
            });
    }

    public CompletableFuture<Integer> readUInt16LE() {
        return waitForData(2)
            .thenApply(v -> {
                int value = (incomingTokenBuffer.get() & 0xFF) | ((incomingTokenBuffer.get() & 0xFF) << 8);
                offset += 2;
                return value;
            });
    }

    public CompletableFuture<Long> readUInt32LE() {
        return waitForData(4)
            .thenApply(v -> {
                long value = (incomingTokenBuffer.get() & 0xFFL) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 8) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 16) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 24);
                offset += 4;
                return value;
            });
    }

    public CompletableFuture<Long> readUInt32BE() {
        return waitForData(4)
            .thenApply(v -> {
                long value = ((incomingTokenBuffer.get() & 0xFFL) << 24) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 16) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 8) |
                             (incomingTokenBuffer.get() & 0xFFL);
                offset += 4;
                return value;
            });
    }

    public CompletableFuture<Long> readUInt64LE() {
        return waitForData(8)
            .thenApply(v -> {
                long value = (incomingTokenBuffer.get() & 0xFFL) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 8) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 16) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 24) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 32) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 40) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 48) |
                             ((incomingTokenBuffer.get() & 0xFFL) << 56);
                offset += 8;
                return value;
            });
    }

    public CompletableFuture<String> readBVarChar() {
        return readUInt8()
            .thenCompose(length -> {
                if (length == 0) {
                    return CompletableFuture.completedFuture("");
                }
                return waitForData(length * 2)
                    .thenApply(v -> {
                        byte[] bytes = new byte[length * 2];
                        incomingTokenBuffer.get(bytes);
                        offset += length * 2;
                        return new String(bytes, java.nio.charset.StandardCharsets.UTF_16LE);
                    });
            });
    }

    public CompletableFuture<String> readUsVarChar() {
        return readUInt16LE()
            .thenCompose(length -> {
                if (length == 0) {
                    return CompletableFuture.completedFuture("");
                }
                return waitForData(length * 2)
                    .thenApply(v -> {
                        byte[] bytes = new byte[length * 2];
                        incomingTokenBuffer.get(bytes);
                        offset += length * 2;
                        return new String(bytes, java.nio.charset.StandardCharsets.UTF_16LE);
                    });
            });
    }
}