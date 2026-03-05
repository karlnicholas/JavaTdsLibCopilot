package org.tdslib.javatdslib.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;
import org.tdslib.javatdslib.packets.PacketType;

/**
 * Handles the TLS handshake process for a TDS connection. This class wraps the {@link SSLEngine}
 * and manages the wrapping and unwrapping of data during the handshake and subsequent encrypted
 * communication.
 */
public class TlsHandshake {
  private SSLEngine sslEngine;
  private ByteBuffer myNetData;
  private ByteBuffer peerNetData;
  private ByteBuffer peerAppData;

  private static final int TDS_HEADER_LENGTH = 8;

  /**
   * Initiates and performs the TLS handshake.
   *
   * @param host The hostname of the server.
   * @param port The port number of the server.
   * @param connection The network connection to use for I/O.
   * @param sslContext The SSL context to create the SSLEngine from.
   * @throws IOException If an I/O error occurs during the handshake.
   */
  public void tlsHandshake(
      String host, int port, NetworkConnection connection, SSLContext sslContext)
      throws IOException {

    this.sslEngine = sslContext.createSSLEngine(host, port);
    this.sslEngine.setUseClientMode(true);

    final SSLSession session = sslEngine.getSession();
    final int bufferSize = Math.max(session.getPacketBufferSize(), 32768);

    myNetData = ByteBuffer.allocate(bufferSize);
    peerNetData = ByteBuffer.allocate(bufferSize);
    peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize());

    peerNetData.flip();
    sslEngine.beginHandshake();
    doHandshake(connection); // Pass the interface down
  }

  private void doHandshake(final NetworkConnection connection) throws IOException {
    SSLEngineResult result;
    SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
    final ByteBuffer dummy = ByteBuffer.allocate(0);
    final ByteBuffer headerBuf = ByteBuffer.allocate(TDS_HEADER_LENGTH);

    while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
        && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {

      switch (handshakeStatus) {
        case NEED_UNWRAP:
          if (!peerNetData.hasRemaining()) {
            peerNetData.clear();
            headerBuf.clear();

            connection.readFullySync(headerBuf); // Replaced raw socket read
            headerBuf.flip();

            final int packetLength = Short.toUnsignedInt(headerBuf.getShort(2));
            final int tlsDataLength = packetLength - TDS_HEADER_LENGTH;

            peerNetData.limit(tlsDataLength);
            connection.readFullySync(peerNetData); // Replaced raw socket read
            peerNetData.flip();
          }

          result = sslEngine.unwrap(peerNetData, peerAppData);

          if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            peerNetData.compact();
            headerBuf.clear();
            connection.readFullySync(headerBuf); // Replaced raw socket read
            headerBuf.flip();

            final int packetLength = Short.toUnsignedInt(headerBuf.getShort(2));
            final int tlsDataLength = packetLength - TDS_HEADER_LENGTH;

            final int limit = peerNetData.position() + tlsDataLength;
            if (limit > peerNetData.capacity()) {
              throw new IOException("Buffer overflow while reading TLS payload");
            }
            peerNetData.limit(limit);
            connection.readFullySync(peerNetData); // Replaced raw socket read

            peerNetData.flip();
          }
          handshakeStatus = result.getHandshakeStatus();
          break;

        case NEED_WRAP:
          myNetData.clear();
          myNetData.position(TDS_HEADER_LENGTH);

          while (handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
            result = sslEngine.wrap(dummy, myNetData);
            handshakeStatus = result.getHandshakeStatus();
            if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
              break;
            }
          }

          myNetData.flip();
          final int totalLength = myNetData.limit();

          myNetData.put(0, PacketType.PRE_LOGIN.getValue());
          myNetData.put(1, (byte) 0x01);
          myNetData.putShort(2, (short) totalLength);
          myNetData.putShort(4, (short) 0x0000);
          myNetData.put(6, (byte) 0x01);
          myNetData.put(7, (byte) 0x00);

          connection.writeDirect(myNetData); // Replaced raw socket write loop
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

  /**
   * Encrypts and writes application data to the network connection.
   *
   * @param appData The application data to encrypt and write.
   * @param connection The network connection to write to.
   * @throws IOException If an I/O error occurs.
   */
  public void writeEncrypted(final ByteBuffer appData, final NetworkConnection connection)
      throws IOException {
    while (appData.hasRemaining()) {
      myNetData.clear();
      final SSLEngineResult result = sslEngine.wrap(appData, myNetData);
      if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
        myNetData = ByteBuffer.allocate(myNetData.capacity() * 2);
        continue;
      }
      myNetData.flip();
      connection.writeDirect(myNetData); // Replaced raw socket write loop
    }
  }

  public boolean isTlsActive() {
    return sslEngine != null;
  }

  /** Closes the TLS handshake and releases resources. */
  public void close() {
    if (sslEngine != null) {
      try {
        sslEngine.closeOutbound();
      } catch (Exception e) {
        // Ignore exceptions during close
      } finally {
        sslEngine = null;
        if (myNetData != null) {
          myNetData.clear();
        }
      }
    }
  }
}
