package org.tdslib.javatdslib.streaming;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;
import org.reactivestreams.Subscriber;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * A generic stream handler for processing Partially Length-Prefixed (PLP) data.
 * This handles the TDS chunking state machine and delegates the data transformation
 * (e.g., to String or ByteBuffer) to the provided transformer function.
 */
public class PlpStreamHandler<T> implements TdsStreamHandler {
  private final TdsTransport transport;
  private final TdsStreamHandler controlPlaneHandler;
  private final Subscriber<? super T> subscriber;
  private final Function<byte[], T> dataTransformer;
  private final java.util.function.Consumer<ByteBuffer> onCompleteCallback;

  private int pendingChunkBytes = 0;
  private boolean expectingChunkLength = true;
  private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);

  public PlpStreamHandler(
      TdsTransport transport,
      TdsStreamHandler controlPlaneHandler,
      Subscriber<? super T> subscriber,
      Function<byte[], T> dataTransformer,
      java.util.function.Consumer<ByteBuffer> onCompleteCallback) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.subscriber = subscriber;
    this.dataTransformer = dataTransformer;
    this.onCompleteCallback = onCompleteCallback;
  }

  @Override
  public void onPayloadAvailable(ByteBuffer payload, boolean isEom) {
    try {
      while (payload.hasRemaining()) {
        if (expectingChunkLength) {
          while (payload.hasRemaining() && lengthBuffer.hasRemaining()) {
            lengthBuffer.put(payload.get());
          }
          if (lengthBuffer.hasRemaining()) return;

          lengthBuffer.flip();
          pendingChunkBytes = lengthBuffer.getInt();
          lengthBuffer.clear();

          // Basic Protocol Validation to prevent client hangs
          if (pendingChunkBytes < 0) {
            throw new IllegalStateException("Invalid PLP chunk length: " + pendingChunkBytes);
          }

          if (pendingChunkBytes == 0) { // PLP Terminator
            subscriber.onComplete();
            transport.setStreamHandlers(controlPlaneHandler, null);
            if (onCompleteCallback != null) {
              onCompleteCallback.accept(payload.slice());
            } else if (payload.hasRemaining()) {
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

          // Transform raw bytes to ByteBuffer or CharSequence and emit
          subscriber.onNext(dataTransformer.apply(chunkData));

          if (pendingChunkBytes == 0) {
            expectingChunkLength = true;
          }
        }
      }
    } catch (Exception e) {
      // Signal error to the LOB consumer
      subscriber.onError(e);
      // Restore the control plane so main decoder can attempt recovery
      transport.setStreamHandlers(controlPlaneHandler, null);
      // Open the network valve so the socket doesn't permanently hang
      transport.resumeNetworkRead();
    }
  }
}