package org.tdslib.javatdslib.transport;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Low-level TCP transport for TDS communication.
 * Handles raw byte I/O over TCP (with optional future TLS support).
 */
public class TcpTransport implements Closeable {

    private final SocketChannel socketChannel;
    private final Socket socket;

    private final int connectTimeoutMs = 30_000;
    private final int readTimeoutMs = 60_000;

    public TcpTransport(String host, int port) throws IOException {
        this.socket = new Socket();
        this.socket.setSoTimeout(readTimeoutMs);
        this.socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        this.socketChannel = socket.getChannel();
        this.socketChannel.configureBlocking(true);
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
                throw new EOFException("Unexpected end of stream");
            }
            if (read == 0) {
                // Should not happen in blocking mode, but safety check
                Thread.onSpinWait();
            }
        }
    }

    /**
     * Flushes any pending data (usually no-op for TCP sockets).
     */
    public void flush() {
        // TCP sockets typically flush automatically, but can be overridden for TLS
    }

    /**
     * Closes the underlying socket.
     */
    @Override
    public void close() throws IOException {
        socket.close();
    }

    /**
     * Placeholder for future TLS support.
     */
    public boolean isSecure() {
        return false; // extend for SSLSocketChannel later
    }

    public int getCurrentPacketSize() {
        return 4096; // default TDS packet size; can be updated after ENVCHANGE
    }

    // Optional: methods to update packet size after login/env change
    public void setPacketSize(int newSize) {
        // Currently no-op; in future can adjust read/write buffers
    }
}