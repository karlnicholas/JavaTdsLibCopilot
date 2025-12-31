// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.io.connection.tcp;

import org.tdslib.javatdslib.io.connection.ConnectionOptions;
import org.tdslib.javatdslib.io.connection.IConnection;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

public class TcpConnection implements IConnection {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TcpConnection.class);
    private final SocketChannel channel;
    private final TcpServerEndpoint endpoint;
    private final TcpConnectionOptions options;

    // TLS fields
    private SSLEngine sslEngine;
    private ByteBuffer myNetData;    // Encrypted data to send to peer
    private ByteBuffer peerNetData;  // Encrypted data received from peer
    private ByteBuffer peerAppData;  // Decrypted data for the application

    private static final int TDS_HEADER_LENGTH = 8;
    private static final int PRELOGIN_PACKET_TYPE = 0x12;

    public TcpConnection(TcpServerEndpoint endpoint) throws IOException {
        this(new TcpConnectionOptions(), endpoint);
    }

    public TcpConnection(TcpConnectionOptions options, TcpServerEndpoint endpoint) throws IOException {
        this.options = options;
        this.endpoint = endpoint;
        this.channel = SocketChannel.open();
        this.channel.configureBlocking(true);
        this.channel.connect(new InetSocketAddress(endpoint.getHostname(), endpoint.getPort()));
    }

    @Override
    public ConnectionOptions getOptions() {
        return options;
    }

    @Override
    public void startTLS() {
        try {
            // 1. Trust All Certs (Explicitly requested)
            TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                        public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                        public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                    }
            };

            // 2. Init SSLContext (TLSv1.2)
            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(null, trustAllCerts, new SecureRandom());

            this.sslEngine = sslContext.createSSLEngine(endpoint.getHostname(), endpoint.getPort());
            this.sslEngine.setUseClientMode(true);

            SSLSession session = sslEngine.getSession();
            // Allocate buffers.
            // peerNetData needs to be large enough to hold TDS packets + TLS records.
            int bufferSize = Math.max(session.getPacketBufferSize(), 32768);

            myNetData = ByteBuffer.allocate(bufferSize);
            peerNetData = ByteBuffer.allocate(bufferSize);
            peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize());

            // Prepare for reading
            peerNetData.flip();

            sslEngine.beginHandshake();
            doHandshake();

        } catch (NoSuchAlgorithmException | IOException | KeyManagementException e) {
            throw new RuntimeException("TLS Handshake failed", e);
        }
    }

    private void doHandshake() throws IOException {
        SSLEngineResult result;
        SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
        ByteBuffer dummy = ByteBuffer.allocate(0);

        // Helper buffer for reading the TDS Header during handshake
        ByteBuffer headerBuf = ByteBuffer.allocate(TDS_HEADER_LENGTH);

        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED &&
                handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {

            switch (handshakeStatus) {
                case NEED_UNWRAP:
                    // HANDSHAKE PHASE: TLS Records are WRAPPED in TDS 0x12 Packets.

                    // Only read from network if we don't have remaining data in the buffer.
                    // (This prevents overwriting a 'Finished' message if it came with the previous packet)
                    if (!peerNetData.hasRemaining()) {
                        peerNetData.clear();
                        headerBuf.clear();

                        // 1. Read TDS Header
                        readFully(headerBuf);
                        headerBuf.flip();

                        // 2. Parse Length (Bytes 2-3 Big Endian)
                        int packetLength = Short.toUnsignedInt(headerBuf.getShort(2));
                        int tlsDataLength = packetLength - TDS_HEADER_LENGTH;

                        // 3. Read the TLS Payload inside the TDS packet
                        peerNetData.limit(tlsDataLength);
                        readFully(peerNetData);
                        peerNetData.flip();
                    }

                    try {
                        result = sslEngine.unwrap(peerNetData, peerAppData);

                        // Handle Buffer Underflow (TLS Record split across TDS packets)
                        if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                            peerNetData.compact();

                            // Read NEXT TDS Header
                            headerBuf.clear();
                            readFully(headerBuf);
                            headerBuf.flip();

                            int packetLength = Short.toUnsignedInt(headerBuf.getShort(2));
                            int tlsDataLength = packetLength - TDS_HEADER_LENGTH;

                            // Read NEXT Payload and append
                            int limit = peerNetData.position() + tlsDataLength;
                            if (limit > peerNetData.capacity()) throw new IOException("Buffer overflow");
                            peerNetData.limit(limit);
                            readFully(peerNetData);

                            peerNetData.flip(); // Retry unwrap
                        }
                    } catch (SSLException e) {
                        throw new IOException("TLS Handshake unwrap failed", e);
                    }
                    handshakeStatus = result.getHandshakeStatus();
                    break;

                case NEED_WRAP:
                    myNetData.clear();
                    // HANDSHAKE PHASE: Reserve 8 bytes for TDS Header
                    myNetData.position(TDS_HEADER_LENGTH);

                    try {
                        result = sslEngine.wrap(dummy, myNetData);
                        handshakeStatus = result.getHandshakeStatus();
                    } catch (SSLException e) {
                        throw new IOException("TLS Handshake wrap failed", e);
                    }

                    myNetData.flip();
                    int totalLength = myNetData.limit();

                    // Add TDS Header (0x12 Pre-Login)
                    myNetData.put(0, (byte) PRELOGIN_PACKET_TYPE);
                    myNetData.put(1, (byte) 0x01);
                    myNetData.putShort(2, (short) totalLength);
                    myNetData.putShort(4, (short) 0x0000);
                    myNetData.put(6, (byte) 0x01);
                    myNetData.put(7, (byte) 0x00);

                    while (myNetData.hasRemaining()) {
                        channel.write(myNetData);
                    }
                    break;

                case NEED_TASK:
                    Runnable task;
                    while ((task = sslEngine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;

                default:
                    throw new IllegalStateException("Invalid TLS Handshake status: " + handshakeStatus);
            }
        }
    }

    private void readFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int count = channel.read(buffer);
            if (count == -1) {
                throw new IOException("Connection closed unexpectedly during handshake");
            }
        }
    }

    @Override
    public void sendData(ByteBuffer byteBuffer) {
        try {
            if (sslEngine != null) {
                sendEncrypted(byteBuffer);
            } else {
                while (byteBuffer.hasRemaining()) {
                    channel.write(byteBuffer);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write data", e);
        }
    }
// ... inside TcpConnection class ...

    // Helper for hex dumping
    private void logHex(String label, ByteBuffer buffer) {
        if (!logger.isDebugEnabled()) return;

        StringBuilder sb = new StringBuilder();
        sb.append(label).append(" (Length: ").append(buffer.remaining()).append(")\n");

        int pos = buffer.position();
        int i = 0;
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            sb.append(String.format("%02X ", b));
            if (++i % 16 == 0) sb.append("\n");
        }
        sb.append("\n");

        // Rewind so the actual write operation can read it again
        buffer.position(pos);
        logger.debug(sb.toString());
    }

    private void sendEncrypted(ByteBuffer appData) throws IOException {
        // DEBUG: Log the PLAINTEXT TDS packet before encryption
        logHex("OUTGOING TDS PACKET (Decrypted View)", appData);

        while (appData.hasRemaining()) {
            myNetData.clear();
            SSLEngineResult result = sslEngine.wrap(appData, myNetData);

            if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                myNetData = ByteBuffer.allocate(myNetData.capacity() * 2);
                continue;
            }

            myNetData.flip();
            while (myNetData.hasRemaining()) {
                channel.write(myNetData);
            }
        }
    }

    @Override
    public ByteBuffer receiveData() {
        try {
            if (sslEngine != null) {
                return receiveDecrypted();
            } else {
                ByteBuffer buffer = ByteBuffer.allocate(4096);
                int bytesRead = channel.read(buffer);
                if (bytesRead == -1) throw new RuntimeException(new IOException("Connection closed by remote host"));
                buffer.flip();
                return buffer;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read data", e);
        }
    }

    private ByteBuffer receiveDecrypted() throws IOException {
        peerAppData.clear();

        while (peerAppData.position() == 0) {
            // POST-HANDSHAKE: Read RAW TLS Records. DO NOT parse TDS Headers.
            if (!peerNetData.hasRemaining()) {
                peerNetData.clear();
                int count = channel.read(peerNetData);
                if (count == -1) throw new IOException("Connection closed during TLS read");
                peerNetData.flip();
            }

            SSLEngineResult result = sslEngine.unwrap(peerNetData, peerAppData);

            if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                // Not enough data for a full TLS record
                peerNetData.compact();
                int count = channel.read(peerNetData);
                if (count == -1) throw new IOException("Connection closed during TLS read");
                peerNetData.flip();
            }
        }

        peerAppData.flip();
        ByteBuffer resultBuffer = ByteBuffer.allocate(peerAppData.remaining());
        resultBuffer.put(peerAppData);
        resultBuffer.flip();
        return resultBuffer;
    }

    @Override
    public void clearIncomingData() {}

    @Override
    public void close() throws IOException {
        if (sslEngine != null) {
            sslEngine.closeOutbound();
        }
        if (channel != null) {
            channel.close();
        }
    }
}