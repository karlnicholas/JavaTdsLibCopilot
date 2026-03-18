package org.tdslib.javatdslib.streaming;

import io.r2dbc.spi.Blob;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TdsBlob implements Blob {
  private final ByteBuffer plpData;
  private java.util.function.Consumer<ByteBuffer> completionListener;

  public TdsBlob(ByteBuffer plpData) {
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
          subscriber,
          ByteBuffer::wrap,
          unconsumedBytes -> {
            completed[0] = true;
            if (completionListener != null) {
              completionListener.accept(unconsumedBytes);
            }
          }
      );

      // 2. NOW ALLOW SUBSCRIPTION
      subscriber.onSubscribe(new Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() {}
      });

      // 3. SYNCHRONOUSLY PROCESS MEMORY BEFORE EVALUATING STARVATION
      if (plpData != null && plpData.hasRemaining()) {
        plpHandler.onPayloadAvailable(plpData, false);
      }
    };
  }

  @Override
  public Publisher<Void> discard() {
    return subscriber -> {
      boolean[] completed = new boolean[1];

      PlpStreamHandler<ByteBuffer> plpHandler = new PlpStreamHandler<>(
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

      subscriber.onSubscribe(new Subscription() {
        @Override public void request(long n) {}
        @Override public void cancel() {}
      });

      if (plpData != null && plpData.hasRemaining()) {
        plpHandler.onPayloadAvailable(plpData, false);
      }

    };
  }
}