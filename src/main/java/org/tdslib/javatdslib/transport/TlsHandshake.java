package org.tdslib.javatdslib.transport;

import org.tdslib.javatdslib.packets.PacketType;

import javax.net.ssl.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TlsHandshake {
  private SSLEngine sslEngine;
  private ByteBuffer myNetData;
  private ByteBuffer peerNetData;
  private ByteBuffer peerAppData;

  private static final int TDS_HEADER_LENGTH = 8;

  public void tlsHandshake(
      String host,
      int port,
      SocketChannel socketChannel,
      SSLContext sslContext
  ) throws IOException {

    // SSLEngine is created using the injected Context
    this.sslEngine = sslContext.createSSLEngine(host, port);
    this.sslEngine.setUseClientMode(true);

    final SSLSession session = sslEngine.getSession();
    final int bufferSize = Math.max(session.getPacketBufferSize(), 32768);

    myNetData = ByteBuffer.allocate(bufferSize);
    peerNetData = ByteBuffer.allocate(bufferSize);
    peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize());

    peerNetData.flip();
    sslEngine.beginHandshake();
    doHandshake(socketChannel);
  }

  // ... (The rest of doHandshake, readFully, writeEncrypted, and close remain completely unchanged) ...

  private void doHandshake(final SocketChannel socketChannel) throws IOException {
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

            readFully(headerBuf, socketChannel);
            headerBuf.flip();

            final int packetLength = Short.toUnsignedInt(headerBuf.getShort(2));
            final int tlsDataLength = packetLength - TDS_HEADER_LENGTH;

            peerNetData.limit(tlsDataLength);
            readFully(peerNetData, socketChannel);
            peerNetData.flip();
          }

          result = sslEngine.unwrap(peerNetData, peerAppData);

          if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            peerNetData.compact();
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
          throw new IllegalStateException("Invalid TLS Handshake status: " + handshakeStatus);
      }
    }
  }

  public void readFully(final ByteBuffer buffer, final SocketChannel socketChannel) throws IOException {
    while (buffer.hasRemaining()) {
      final int read = socketChannel.read(buffer);
      if (read == -1) throw new IOException("Unexpected end of stream");
    }
  }

  public void writeEncrypted(final ByteBuffer appData, final SocketChannel socketChannel) throws IOException {
    while (appData.hasRemaining()) {
      myNetData.clear();
      final SSLEngineResult result = sslEngine.wrap(appData, myNetData);
      if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
        myNetData = ByteBuffer.allocate(myNetData.capacity() * 2);
        continue;
      }
      myNetData.flip();
      while (myNetData.hasRemaining()) {
        socketChannel.write(myNetData);
      }
    }
  }

  public boolean isTlsActive() { return sslEngine != null; }

  public void close() {
    if (sslEngine != null) {
      try {
        sslEngine.closeOutbound();
      } catch (Exception e) {
      } finally {
        sslEngine = null;
        if (myNetData != null) myNetData.clear();
      }
    }
  }
}