package org.tdslib.javatdslib.streaming;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.reactivestreams.Subscriber;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * A stream handler for processing Partially Length-Prefixed (PLP) character data. This handler
 * reads chunks of data from the TDS stream, decodes them using the specified charset, and emits
 * them as {@link CharSequence}s to a subscriber.
 */
public class PlpStreamHandler implements TdsStreamHandler {

  private final TdsTransport transport;
  private final TdsStreamHandler controlPlaneHandler;
  private final Subscriber<? super CharSequence> subscriber;

  private int pendingChunkBytes = 0;
  private boolean expectingChunkLength = true;
  private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

  private final java.nio.charset.Charset charset;

  /**
   * Constructs a new PlpStreamHandler.
   *
   * @param transport The transport layer for reading data.
   * @param controlPlaneHandler The handler to switch back to after processing the PLP data.
   * @param subscriber The subscriber to receive the decoded character data.
   * @param charset The charset used to decode the character data.
   */
  public PlpStreamHandler(
      TdsTransport transport,
      TdsStreamHandler controlPlaneHandler,
      Subscriber<? super CharSequence> subscriber,
      java.nio.charset.Charset charset) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.subscriber = subscriber;
    this.charset = charset;
  }

  @Override
  public void onPayloadAvailable(ByteBuffer payload, boolean isEom) {
    while (payload.hasRemaining()) {

      // STATE 1: Read the 4-byte chunk length
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

        // Check for PLP Terminator
        if (pendingChunkBytes == 0) {
          subscriber.onComplete();

          // Swap back to the Control Plane
          transport.setStreamHandlers(controlPlaneHandler, null);

          // Pass any leftover bytes (like the DONE token) back to the StatefulTokenDecoder
          if (payload.hasRemaining()) {
            controlPlaneHandler.onPayloadAvailable(payload, isEom);
          }
          return;
        }
        expectingChunkLength = false;
      }

      // STATE 2: Read and emit chunk data
      if (!expectingChunkLength && pendingChunkBytes > 0) {
        int bytesToRead = Math.min(payload.remaining(), pendingChunkBytes);
        byte[] chunkData = new byte[bytesToRead];
        payload.get(chunkData);
        pendingChunkBytes -= bytesToRead;

        // MVC specific: Assume VARCHAR(MAX)/NVARCHAR(MAX) is UTF-16LE
        // subscriber.onNext(new String(chunkData, StandardCharsets.UTF_16LE));
        subscriber.onNext(new String(chunkData, charset));

        if (pendingChunkBytes == 0) {
          expectingChunkLength = true;
        }
      }
    }
  }
}
