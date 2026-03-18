package org.tdslib.javatdslib.streaming;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

public class PlpStreamHandler<T> implements TdsStreamHandler {
  private static final Logger logger = LoggerFactory.getLogger(PlpStreamHandler.class);
  private final Subscriber<? super T> subscriber;
  private final Function<byte[], T> dataTransformer;
  private final java.util.function.Consumer<ByteBuffer> onCompleteCallback;

  private int pendingChunkBytes = 0;
  private boolean expectingChunkLength = true;

  public PlpStreamHandler(
      Subscriber<? super T> subscriber,
      Function<byte[], T> dataTransformer,
      java.util.function.Consumer<ByteBuffer> onCompleteCallback) {
    this.subscriber = subscriber;
    this.dataTransformer = dataTransformer;
    this.onCompleteCallback = onCompleteCallback;
  }

  @Override
  public synchronized void onPayloadAvailable(ByteBuffer payload, boolean isEom) {
    try {
      while (payload.hasRemaining()) {
        if (expectingChunkLength) {
          byte[] lenBytes = new byte[4];
          payload.get(lenBytes);
          pendingChunkBytes = ByteBuffer.wrap(lenBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

          if (pendingChunkBytes < 0) {
            throw new IllegalStateException("Invalid PLP chunk length: " + pendingChunkBytes);
          }

          if (pendingChunkBytes == 0) { // PLP Terminator
            logger.trace("[PlpStreamHandler] Terminator reached. Releasing router control before signaling downstream.");

            // 1. CLEANUP INTERNAL STATE FIRST
            if (onCompleteCallback != null) {
              ByteBuffer remainingBytes = payload.slice();

              // CRITICAL FIX: Advance the parent buffer's position to its limit.
              // This tells the NioSocketConnection that we have completely taken ownership
              // of the remaining packet bytes, preventing them from being double-fed
              // on the next Event Loop cycle.
              payload.position(payload.limit());

              onCompleteCallback.accept(remainingBytes);
            }

            // 2. NOW SIGNAL THE CLIENT
            subscriber.onComplete();

            return;
          }
          expectingChunkLength = false;
        }

        if (!expectingChunkLength && pendingChunkBytes > 0) {
          int bytesToRead = Math.min(payload.remaining(), pendingChunkBytes);
          byte[] chunkData = new byte[bytesToRead];
          payload.get(chunkData);
          pendingChunkBytes -= bytesToRead;

          subscriber.onNext(dataTransformer.apply(chunkData));

          if (pendingChunkBytes == 0) {
            expectingChunkLength = true;
          }
        }
      }
    } catch (Exception e) {
      logger.error("[PlpStreamHandler] Fatal error during LOB parsing", e);
      // Ensure router is safely reverted on error
      subscriber.onError(e);
    }
  }
}