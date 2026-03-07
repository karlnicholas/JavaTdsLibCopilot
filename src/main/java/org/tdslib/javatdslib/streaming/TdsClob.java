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

  // Inside TdsClob.java
  @Override
  public Publisher<CharSequence> stream() {
    return subscriber -> {

      subscriber.onSubscribe(new Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() { transport.resumeNetworkRead(); }
      });

      // USE THE UNIFIED HANDLER
      PlpStreamHandler<CharSequence> plpHandler = new PlpStreamHandler<>(
          transport,
          controlPlaneHandler,
          subscriber,
          bytes -> new String(bytes, charset), // <--- Transforms byte[] to String using the charset
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
  public Publisher<Void> discard() {
    return null;
  }
}