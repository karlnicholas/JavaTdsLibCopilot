package org.tdslib.javatdslib.streaming;

import io.r2dbc.spi.Blob;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.nio.ByteBuffer;

public class TdsBlob implements Blob {

  private final TdsTransport transport;
  private final TdsStreamHandler controlPlaneHandler;
  private final ByteBuffer leftoverBytes;

  public TdsBlob(TdsTransport transport, TdsStreamHandler controlPlaneHandler, ByteBuffer leftoverBytes) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.leftoverBytes = leftoverBytes;
  }

  @Override
  public Publisher<ByteBuffer> stream() {
    return subscriber -> {
      PlpBlobStreamHandler plpHandler = new PlpBlobStreamHandler(transport, controlPlaneHandler, subscriber);

      // Hijack the network stream
      transport.setStreamHandlers(plpHandler, null);

      // Feed the leftover bytes FIRST!
      if (leftoverBytes != null && leftoverBytes.hasRemaining()) {
        plpHandler.onPayloadAvailable(leftoverBytes, false);
      }

      // Open the valve for future packets!
      transport.resumeNetworkRead();
    };
  }

  @Override
  public Publisher<Void> discard() {
    return null; // Omitted for MVC
  }
}