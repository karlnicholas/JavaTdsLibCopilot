// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.io.connection.tcp;

import org.tdslib.javatdslib.io.connection.ConnectionOptions;
import org.tdslib.javatdslib.io.connection.IConnection;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.CompletableFuture;

/**
 * Connection used by the TDS Client to communicate with the SQL Server.
 */
public class TcpConnection implements IConnection {
    private final AsynchronousSocketChannel channel;
    private final TcpServerEndpoint endpoint;
    private final TcpConnectionOptions options;

    /**
     * TCP Connection options.
     */
    public TcpConnectionOptions getTcpOptions() {
        return options;
    }

    /**
     * Create a new TCP connection with default options.
     */
    public TcpConnection(TcpServerEndpoint endpoint) throws IOException {
        this(new TcpConnectionOptions(), endpoint);
    }

    /**
     * Create a new TCP connection with specified options.
     */
    public TcpConnection(TcpConnectionOptions options, TcpServerEndpoint endpoint) throws IOException {
        this.options = options;
        this.endpoint = endpoint;
        this.channel = AsynchronousSocketChannel.open();
        // Connect synchronously for simplicity, or make async
        try {
            channel.connect(new InetSocketAddress(endpoint.getHostname(), endpoint.getPort())).get();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public ConnectionOptions getOptions() {
        return options;
    }

    @Override
    public CompletableFuture<Void> startTLS() {
        // Implement TLS handshake
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> sendData(ByteBuffer byteBuffer) {
        return CompletableFuture.runAsync(() -> {
            try {
                while (byteBuffer.hasRemaining()) {
                    channel.write(byteBuffer).get();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<ByteBuffer> receiveData() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(4096); // or options.getPacketSize()
                int bytesRead = channel.read(buffer).get();
                if (bytesRead == -1) {
                    throw new IOException("Connection closed");
                }
                buffer.flip();
                return buffer;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void clearIncomingData() {
        // Implement if needed
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }
}