package org.tdslib.javatdslib.streaming;

import io.r2dbc.spi.Blob;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * Implementation of {@link Blob} for the TDS protocol. This class represents a Binary Large Object
 * (BLOB) that can be streamed from the database. It handles the streaming of data chunks using the
 * underlying TDS transport.
 */
public class TdsBlob implements Blob {

  private final TdsTransport transport;
  private final TdsStreamHandler controlPlaneHandler;
  private final ByteBuffer leftoverBytes;

  /**
   * Constructs a new TdsBlob.
   *
   * @param transport The transport layer for reading BLOB data.
   * @param controlPlaneHandler The handler to switch back to after the BLOB is fully read.
   * @param leftoverBytes Any bytes already read from the stream that belong to this BLOB.
   */
  public TdsBlob(
      TdsTransport transport, TdsStreamHandler controlPlaneHandler, ByteBuffer leftoverBytes) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.leftoverBytes = leftoverBytes;
  }

  @Override
  public Publisher<ByteBuffer> stream() {
    return subscriber -> {
      PlpBlobStreamHandler plpHandler =
          new PlpBlobStreamHandler(transport, controlPlaneHandler, subscriber);

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
