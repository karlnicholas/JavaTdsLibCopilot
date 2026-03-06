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

  public TdsBlob(
      TdsTransport transport, TdsStreamHandler controlPlaneHandler, ByteBuffer plpData) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.plpData = plpData; // Now holds the fully extracted clean data
  }

  @Override
  public Publisher<ByteBuffer> stream() {
    return new Publisher<ByteBuffer>() {
      @Override
      public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) {
            if (n > 0 && plpData != null && plpData.hasRemaining()) {
              // Emit the buffered PLP data instantly when requested
              subscriber.onNext(plpData.duplicate());
              plpData.position(plpData.limit());
              subscriber.onComplete();
            } else if (n > 0) {
              subscriber.onComplete();
            }
          }

          @Override
          public void cancel() {}
        });
      }
    };
  }

  @Override
  public Publisher<Void> discard() {
    return null;
  }
}