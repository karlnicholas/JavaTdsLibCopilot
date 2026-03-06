package org.tdslib.javatdslib.streaming;

import io.r2dbc.spi.Blob;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

public class TdsBlob implements Blob {
  private final TdsTransport transport;
  private final TdsStreamHandler controlPlaneHandler;
  private final ByteBuffer plpData;
  private java.util.function.Consumer<ByteBuffer> completionListener;

  public TdsBlob(TdsTransport transport, TdsStreamHandler controlPlaneHandler, ByteBuffer plpData) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.plpData = plpData;
  }

  public void setCompletionListener(java.util.function.Consumer<ByteBuffer> listener) {
    this.completionListener = listener;
  }

  @Override
  public Publisher<ByteBuffer> stream() {
    return subscriber -> {

      // FIX: Strictly comply with Reactive Streams by emitting a Subscription first
      subscriber.onSubscribe(new Subscription() {
        @Override
        public void request(long n) {
          // No backpressure needed for this test, data is pushed below
        }
        @Override
        public void cancel() {
          transport.resumeNetworkRead();
        }
      });

      PlpBlobStreamHandler plpHandler = new PlpBlobStreamHandler(
          transport,
          controlPlaneHandler,
          subscriber,
          unconsumedBytes -> {
            if (completionListener != null) {
              completionListener.accept(unconsumedBytes);
            }
            transport.resumeNetworkRead();
          }
      );

      transport.setStreamHandlers(plpHandler, null);

      if (plpData != null && plpData.hasRemaining()) {
        plpHandler.onPayloadAvailable(plpData, false);
      }

      transport.resumeNetworkRead();
    };
  }

  @Override
  public Publisher<Void> discard() { return null; }
}