package org.tdslib.javatdslib.streaming;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.reactivestreams.Subscriber;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * A stream handler for processing Partially Length-Prefixed (PLP) character data.
 */
public class PlpClobStreamHandler implements TdsStreamHandler {

  private final TdsTransport transport;
  private final TdsStreamHandler controlPlaneHandler;
  private final Subscriber<? super CharSequence> subscriber;
  private final java.nio.charset.Charset charset;

  private int pendingChunkBytes = 0;
  private boolean expectingChunkLength = true;
  private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

  // Callback to hand trailing bytes back to the Row
  private final java.util.function.Consumer<ByteBuffer> onCompleteCallback;

  public PlpClobStreamHandler(
      TdsTransport transport,
      TdsStreamHandler controlPlaneHandler,
      Subscriber<? super CharSequence> subscriber,
      java.nio.charset.Charset charset,
      java.util.function.Consumer<ByteBuffer> onCompleteCallback) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.subscriber = subscriber;
    this.charset = charset;
    this.onCompleteCallback = onCompleteCallback;
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

        System.out.println(">>> CLOB HANDLER: Read chunk length = " + pendingChunkBytes);

        // Check for PLP Terminator
        if (pendingChunkBytes == 0) {
          subscriber.onComplete();

          // Swap back to the Control Plane
          transport.setStreamHandlers(controlPlaneHandler, null);

          // Hand the trailing bytes back to StatefulRow!
          if (onCompleteCallback != null) {
            onCompleteCallback.accept(payload.slice());
          } else if (payload.hasRemaining()) {
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

        System.out.println(">>> CLOB HANDLER: Emitting " + bytesToRead + " raw bytes to subscriber!");
        subscriber.onNext(new String(chunkData, charset));

        if (pendingChunkBytes == 0) {
          expectingChunkLength = true;
        }
      }
    }
  }
}