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
      boolean[] completed = new boolean[1];

      PlpStreamHandler<CharSequence> plpHandler = new PlpStreamHandler<>(
          transport,
          controlPlaneHandler,
          subscriber,
          bytes -> new String(bytes, charset),
          unconsumedBytes -> {
            completed[0] = true;
            if (completionListener != null) {
              completionListener.accept(unconsumedBytes);
            }
          }
      );

      transport.switchStreamHandler(plpHandler);

      subscriber.onSubscribe(new Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() {
          transport.switchStreamHandler(controlPlaneHandler);
          transport.resumeNetworkRead();
        }
      });

      // Synchronously process memory BEFORE evaluating starvation
      if (plpData != null && plpData.hasRemaining()) {
        plpHandler.onPayloadAvailable(plpData, false);
      }

      // ONLY WAKE NETWORK IF LOB IS STARVING
      if (!completed[0]) {
        transport.resumeNetworkRead();
      }
    };
  }

  @Override
  public Publisher<Void> discard() {
    return subscriber -> {
      boolean[] completed = new boolean[1];

      PlpStreamHandler<CharSequence> plpHandler = new PlpStreamHandler<>(
          transport,
          controlPlaneHandler,
          new org.reactivestreams.Subscriber<CharSequence>() {
            @Override public void onSubscribe(Subscription s) {}
            @Override public void onNext(CharSequence charSequence) {}
            @Override public void onError(Throwable t) { subscriber.onError(t); }
            @Override public void onComplete() { subscriber.onComplete(); }
          },
          bytes -> "",
          unconsumedBytes -> {
            completed[0] = true;
            if (completionListener != null) {
              completionListener.accept(unconsumedBytes);
            }
          }
      );

      transport.switchStreamHandler(plpHandler);

      subscriber.onSubscribe(new Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() {}
      });

      if (plpData != null && plpData.hasRemaining()) {
        plpHandler.onPayloadAvailable(plpData, false);
      }

      if (!completed[0]) {
        transport.resumeNetworkRead();
      }
    };
  }
}