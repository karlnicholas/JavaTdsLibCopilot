package org.tdslib.javatdslib.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Low-level TCP transport for TDS communication.
 * Handles raw byte I/O over TCP (blocking mode).
 */
public class TcpTransport implements AutoCloseable {

    private final SocketChannel socketChannel;

    private final int connectTimeoutMs = 30_000;
    private final int readTimeoutMs = 60_000;

    public TcpTransport(String host, int port) throws IOException {
        this.socketChannel = SocketChannel.open();
        this.socketChannel.configureBlocking(true);
        this.socketChannel.socket().setSoTimeout(readTimeoutMs);

        // Connect with timeout
        if (!socketChannel.connect(new InetSocketAddress(host, port))) {
            // If non-blocking connect, finish it (but we use blocking)
            socketChannel.finishConnect();
        }
    }

    /**
     * Writes a full ByteBuffer to the socket.
     */
    public void write(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }
    }

    /**
     * Reads exactly 'size' bytes into the provided buffer.
     * Blocks until all requested bytes are available or EOF/error occurs.
     */
    public void readFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = socketChannel.read(buffer);
            if (read == -1) {
                throw new IOException("Unexpected end of stream");
            }
            if (read == 0) {
                // Rare in blocking mode, but safety
                Thread.onSpinWait();
            }
        }
    }

    /**
     * Flushes any pending data (no-op for plain TCP).
     */
    public void flush() {
        // No-op for plain TCP sockets
    }

    /**
     * Closes the underlying channel/socket.
     */
    @Override
    public void close() throws IOException {
        socketChannel.close();
    }

    public boolean isSecure() {
        return false; // extend for TLS later
    }

    public int getCurrentPacketSize() {
        return 4096; // default; update after ENVCHANGE
    }

    public void setPacketSize(int newSize) {
        // Future: adjust read/write buffers if needed
        // Currently no-op
    }
}