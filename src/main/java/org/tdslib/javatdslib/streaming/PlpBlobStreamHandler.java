package org.tdslib.javatdslib.streaming;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.reactivestreams.Subscriber;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * A stream handler for processing Partially Length-Prefixed (PLP) BLOB data. This handler reads
 * chunks of data from the TDS stream and emits them as {@link ByteBuffer}s to a subscriber.
 */
public class PlpBlobStreamHandler implements TdsStreamHandler {

  private final TdsTransport transport;
  private final TdsStreamHandler controlPlaneHandler;
  private final Subscriber<? super ByteBuffer> subscriber;

  private int pendingChunkBytes = 0;
  private boolean expectingChunkLength = true;
  private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

  /**
   * Constructs a new PlpBlobStreamHandler.
   *
   * @param transport The transport layer for reading data.
   * @param controlPlaneHandler The handler to switch back to after processing the BLOB.
   * @param subscriber The subscriber to receive the BLOB data chunks.
   */
  public PlpBlobStreamHandler(
      TdsTransport transport,
      TdsStreamHandler controlPlaneHandler,
      Subscriber<? super ByteBuffer> subscriber) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.subscriber = subscriber;
  }

  @Override
  public void onPayloadAvailable(ByteBuffer payload, boolean isEom) {
    while (payload.hasRemaining()) {

      if (expectingChunkLength) {
        while (payload.hasRemaining() && lengthBuffer.hasRemaining()) {
          lengthBuffer.put(payload.get());
        }
        if (lengthBuffer.hasRemaining()) {
          return; // Wait for next packet
        }

        lengthBuffer.flip();
        pendingChunkBytes = lengthBuffer.getInt();
        lengthBuffer.clear();

        if (pendingChunkBytes == 0) {
          subscriber.onComplete();

          transport.setStreamHandlers(controlPlaneHandler, null);

          if (payload.hasRemaining()) {
            controlPlaneHandler.onPayloadAvailable(payload, isEom);
          }
          return;
        }
        expectingChunkLength = false;
      }

      if (!expectingChunkLength && pendingChunkBytes > 0) {
        int bytesToRead = Math.min(payload.remaining(), pendingChunkBytes);
        byte[] chunkData = new byte[bytesToRead];
        payload.get(chunkData);
        pendingChunkBytes -= bytesToRead;

        // EMIT RAW BYTES DIRECTLY
        subscriber.onNext(ByteBuffer.wrap(chunkData));

        if (pendingChunkBytes == 0) {
          expectingChunkLength = true;
        }
      }
    }
  }
}
