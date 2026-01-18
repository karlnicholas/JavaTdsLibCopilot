package org.tdslib.javatdslib.transport;

import org.tdslib.javatdslib.packets.PacketType;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * Performs a TLS handshake over a {@link SocketChannel} for the TDS protocol.
 *
 * <p>This class configures an {@link SSLEngine} in client mode, sets up buffers
 * for TLS records and application data, and drives the TLS handshake by
 * exchanging TDS-wrapped TLS records with the peer.</p>
 *
 * <p><b>Important:</b> The current implementation installs a TrustManager that
 * accepts all certificates (trusts all certs). This is insecure and intended
 * only for testing or explicit use cases where certificate validation is not
 * required.</p>
 */
public class TlsHandshake {
  // TLS fields (null if not using TLS)
  private SSLEngine sslEngine;
  private ByteBuffer myNetData;     // Outgoing encrypted data
  private ByteBuffer peerNetData;   // Incoming encrypted data
  private ByteBuffer peerAppData;   // Decrypted application data

  private static final int TDS_HEADER_LENGTH = 8;

  /**
   * Enable TLS on the existing connection and perform the TLS handshake.
   *
   * @throws IOException on TLS initialization or handshake failures
   */
  public void tlsHandshake(
      String host,
      int port,
      SocketChannel socketChannel
  ) throws IOException, NoSuchAlgorithmException, KeyManagementException {
    // 1. Trust All Certs (Explicitly requested)
    final TrustManager[] trustAllCerts = new TrustManager[]{
        new X509TrustManager() {
          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }

          @Override
          public void checkClientTrusted(final X509Certificate[] certs,
                                         final String authType) {
            // Trust all
          }

          @Override
          public void checkServerTrusted(final X509Certificate[] certs,
                                         final String authType) {
            // Trust all
          }
        }
    };

    // 2. Init SSLContext (TLSv1.2)
    final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
    sslContext.init(null, trustAllCerts, new SecureRandom());

    this.sslEngine = sslContext.createSSLEngine(host, port);
    this.sslEngine.setUseClientMode(true);

    final SSLSession session = sslEngine.getSession();
    // Allocate buffers. peerNetData must hold TDS packets + TLS records.
    final int bufferSize = Math.max(session.getPacketBufferSize(), 32768);

    myNetData = ByteBuffer.allocate(bufferSize);
    peerNetData = ByteBuffer.allocate(bufferSize);
    peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize());

    // Prepare for reading
    peerNetData.flip();

    sslEngine.beginHandshake();
    doHandshake(socketChannel);
  }

  private void doHandshake(final SocketChannel socketChannel) throws IOException {
    SSLEngineResult result;
    SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
    final ByteBuffer dummy = ByteBuffer.allocate(0);

    // Helper buffer for reading the TDS header during handshake
    final ByteBuffer headerBuf = ByteBuffer.allocate(TDS_HEADER_LENGTH);

    while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
        && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {

      switch (handshakeStatus) {
        case NEED_UNWRAP:
          // TLS records are wrapped inside TDS 0x12 packets.

          // Only read from network if buffer has no remaining data.
          if (!peerNetData.hasRemaining()) {
            peerNetData.clear();
            headerBuf.clear();

            // 1. Read TDS header
            readFully(headerBuf, socketChannel);
            headerBuf.flip();

            // 2. Parse length (bytes 2-3 big-endian)
            final int packetLength = Short.toUnsignedInt(headerBuf.getShort(2));
            final int tlsDataLength = packetLength - TDS_HEADER_LENGTH;

            // 3. Read the TLS payload inside the TDS packet
            peerNetData.limit(tlsDataLength);
            readFully(peerNetData, socketChannel);
            peerNetData.flip();
          }

          result = sslEngine.unwrap(peerNetData, peerAppData);

          // Handle BUFFER_UNDERFLOW (TLS record split across TDS packets)
          if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            peerNetData.compact();

            // Read next TDS header
            headerBuf.clear();
            readFully(headerBuf, socketChannel);
            headerBuf.flip();

            final int packetLength = Short.toUnsignedInt(headerBuf.getShort(2));
            final int tlsDataLength = packetLength - TDS_HEADER_LENGTH;

            final int limit = peerNetData.position() + tlsDataLength;
            if (limit > peerNetData.capacity()) {
              throw new IOException("Buffer overflow while reading TLS payload");
            }
            peerNetData.limit(limit);
            readFully(peerNetData, socketChannel);

            peerNetData.flip(); // Retry unwrap
          }
          handshakeStatus = result.getHandshakeStatus();
          break;

        case NEED_WRAP:
          myNetData.clear();
          // Reserve 8 bytes for TDS header
          myNetData.position(TDS_HEADER_LENGTH);

          result = sslEngine.wrap(dummy, myNetData);
          handshakeStatus = result.getHandshakeStatus();

          myNetData.flip();
          final int totalLength = myNetData.limit();

          // Add TDS header (0x12 Pre-Login)
          myNetData.put(0, PacketType.PRE_LOGIN.getValue());
          myNetData.put(1, (byte) 0x01);
          myNetData.putShort(2, (short) totalLength);
          myNetData.putShort(4, (short) 0x0000);
          myNetData.put(6, (byte) 0x01);
          myNetData.put(7, (byte) 0x00);

          while (myNetData.hasRemaining()) {
            socketChannel.write(myNetData);
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
          throw new IllegalStateException(
              "Invalid TLS Handshake status: " + handshakeStatus);
      }
    }
  }

  /**
   * Reads a full TDS packet (including header) into the provided buffer.
   * For TLS, this returns the decrypted application data.
   *
   * @param buffer destination buffer
   * @throws IOException on I/O error or end-of-stream
   */
  public void readFully(
      final ByteBuffer buffer,
      final SocketChannel socketChannel
  ) throws IOException {
    while (buffer.hasRemaining()) {
      final int read = socketChannel.read(buffer);
      if (read == -1) {
        throw new IOException("Unexpected end of stream");
      }
    }
  }

  /**
   * Encrypts and sends application data using the configured {@link SSLEngine}.
   *
   * <p>Consumes bytes from {@code appData} by calling {@link SSLEngine#wrap} and
   * writes the resulting TLS records to {@code socketChannel}. If the SSLEngine
   * signals {@link SSLEngineResult.Status#BUFFER_OVERFLOW} the internal network
   * buffer is grown and the wrap is retried. The method returns when all bytes
   * from {@code appData} have been consumed.</p>
   *
   * @param appData       buffer containing plaintext application data; its position
   *                      is advanced as bytes are consumed
   * @param socketChannel destination channel to write encrypted TLS records to
   * @throws IOException on socket I/O errors or if the SSLEngine produces an I/O-related error
   */
  public void writeEncrypted(
      final ByteBuffer appData,
      final SocketChannel socketChannel
  ) throws IOException {
    while (appData.hasRemaining()) {
      myNetData.clear();

      final SSLEngineResult result = sslEngine.wrap(appData, myNetData);

      if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
        // Double buffer size and retry
        myNetData = ByteBuffer.allocate(myNetData.capacity() * 2);
        continue;
      }

      myNetData.flip();
      while (myNetData.hasRemaining()) {
        socketChannel.write(myNetData);
      }
    }
  }

  /**
   * Initiates a TLS close by closing the SSLEngine outbound side.
   *
   * <p>This sends a TLS close_notify when the engine is driven; it does not close
   * the underlying SocketChannel. Callers are responsible for closing the socket.
   *
   * @throws IOException if an I/O error occurs while initiating close
   */
  public void close() throws IOException {
    if (sslEngine != null) {
      sslEngine.closeOutbound();
    }
  }

}
