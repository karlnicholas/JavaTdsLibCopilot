// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.io.connection.tcp;

import org.tdslib.javatdslib.io.connection.ConnectionOptions;
import org.tdslib.javatdslib.io.connection.IConnection;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Connection used by the TDS Client to communicate with the SQL Server.
 */
public class TcpConnection implements IConnection {
    private final SocketChannel channel;
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

        // Open a standard SocketChannel
        this.channel = SocketChannel.open();

        // Explicitly enable blocking mode
        this.channel.configureBlocking(true);

        // Connect synchronously
        this.channel.connect(new InetSocketAddress(endpoint.getHostname(), endpoint.getPort()));
    }

    @Override
    public ConnectionOptions getOptions() {
        return options;
    }

    @Override
    public void startTLS() {
        // TODO: Implement blocking TLS handshake (e.g., using SSLEngine or wrapping the Socket)
    }

    @Override
    public void sendData(ByteBuffer byteBuffer) {
        try {
            // In blocking mode, write() blocks until bytes are written.
            // However, it might not write the *full* buffer in one go if the OS buffer is full.
            while (byteBuffer.hasRemaining()) {
                channel.write(byteBuffer);
            }
        } catch (IOException e) {
            // Interface does not throw checked exceptions for sendData
            throw new RuntimeException("Failed to write data to SocketChannel", e);
        }
    }

    @Override
    public ByteBuffer receiveData() {
        try {
            // Allocate buffer (size could be dynamic based on TDS packet size in options)
            ByteBuffer buffer = ByteBuffer.allocate(4096);

            // Blocks until at least one byte is read or EOF
            int bytesRead = channel.read(buffer);

            if (bytesRead == -1) {
                throw new RuntimeException(new IOException("Connection closed by remote host"));
            }

            // Flip the buffer to prepare it for reading by the caller
            buffer.flip();
            return buffer;
        } catch (IOException e) {
            // Interface does not throw checked exceptions for receiveData
            throw new RuntimeException("Failed to read data from SocketChannel", e);
        }
    }

    @Override
    public void clearIncomingData() {
        // Optional: Implement logic to discard pending bytes if necessary
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }
}