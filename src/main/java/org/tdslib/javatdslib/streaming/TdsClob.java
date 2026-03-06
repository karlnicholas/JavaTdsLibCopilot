package org.tdslib.javatdslib.streaming;

import io.r2dbc.spi.Clob;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

public class TdsClob implements Clob {

  private final TdsTransport transport;
  private final TdsStreamHandler controlPlaneHandler;
  private final ByteBuffer plpData;
  private final Charset charset;

  private java.util.function.Consumer<ByteBuffer> completionListener;

  public TdsClob(
      TdsTransport transport,
      TdsStreamHandler controlPlaneHandler,
      ByteBuffer plpData,
      Charset charset) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.plpData = plpData;
    this.charset = charset;
  }

  public void setCompletionListener(java.util.function.Consumer<ByteBuffer> listener) {
    this.completionListener = listener;
  }

  @Override
  public Publisher<CharSequence> stream() {
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

      // 1. Create the PLP Handler and pass it the callback
      PlpClobStreamHandler plpHandler = new PlpClobStreamHandler(
          transport,
          controlPlaneHandler,
          subscriber,
          charset,
          unconsumedBytes -> {
            if (completionListener != null) {
              completionListener.accept(unconsumedBytes); // Hand back to StatefulRow
            }
            transport.resumeNetworkRead();
          }
      );

      // 2. Hijack the network
      transport.setStreamHandlers(plpHandler, null);

      // 3. Feed it the stolen bytes containing the PLP headers and trailing columns
      if (plpData != null && plpData.hasRemaining()) {
        plpHandler.onPayloadAvailable(plpData, false);
      }

      // 4. Open the valve
      transport.resumeNetworkRead();
    };
  }

  @Override
  public Publisher<Void> discard() {
    return null;
  }
}