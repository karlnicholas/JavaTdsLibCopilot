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

  // Inside TdsBlob.java
  @Override
  public Publisher<ByteBuffer> stream() {
    return subscriber -> {

      subscriber.onSubscribe(new Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() { transport.resumeNetworkRead(); }
      });

      // USE THE UNIFIED HANDLER
      PlpStreamHandler<ByteBuffer> plpHandler = new PlpStreamHandler<>(
          transport,
          controlPlaneHandler,
          subscriber,
          ByteBuffer::wrap, // <--- Transforms byte[] to ByteBuffer
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