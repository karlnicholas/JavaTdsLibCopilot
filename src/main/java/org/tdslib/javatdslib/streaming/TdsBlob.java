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
      boolean[] completed = new boolean[1];

      // 1. SETUP ROUTING FIRST
      PlpStreamHandler<ByteBuffer> plpHandler = new PlpStreamHandler<>(
          transport,
          controlPlaneHandler,
          subscriber,
          ByteBuffer::wrap,
          unconsumedBytes -> {
            completed[0] = true;
            if (completionListener != null) {
              completionListener.accept(unconsumedBytes);
            }
          }
      );

      // Lock in the routing target
      transport.switchStreamHandler(plpHandler);

      // 2. NOW ALLOW SUBSCRIPTION
      subscriber.onSubscribe(new Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() {
          transport.switchStreamHandler(controlPlaneHandler);
          transport.resumeNetworkRead();
        }
      });

      // 3. SYNCHRONOUSLY PROCESS MEMORY BEFORE EVALUATING STARVATION
      if (plpData != null && plpData.hasRemaining()) {
        plpHandler.onPayloadAvailable(plpData, false);
      }

      // 4. ONLY WAKE NETWORK IF LOB IS STARVING
      if (!completed[0]) {
        transport.resumeNetworkRead();
      }
    };
  }

  @Override
  public Publisher<Void> discard() {
    return subscriber -> {
      boolean[] completed = new boolean[1];

      PlpStreamHandler<ByteBuffer> plpHandler = new PlpStreamHandler<>(
          transport,
          controlPlaneHandler,
          new Subscriber<ByteBuffer>() {
            @Override public void onSubscribe(Subscription s) {}
            @Override public void onNext(ByteBuffer byteBuffer) {}
            @Override public void onError(Throwable t) { subscriber.onError(t); }
            @Override public void onComplete() { subscriber.onComplete(); }
          },
          ByteBuffer::wrap,
          unconsumedBytes -> {
            completed[0] = true;
            if (completionListener != null) {
              completionListener.accept(unconsumedBytes);
            }
          }
      );

      // Lock in routing before subscription
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