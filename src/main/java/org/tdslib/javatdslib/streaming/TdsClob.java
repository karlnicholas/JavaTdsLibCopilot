package org.tdslib.javatdslib.streaming;

import io.r2dbc.spi.Clob;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * Implementation of {@link Clob} for the TDS protocol. This class represents a Character Large
 * Object (CLOB) backed by memory. It handles the emission of character data
 * decoded using the specified charset.
 */
public class TdsClob implements Clob {

  private final TdsTransport transport;
  private final TdsStreamHandler controlPlaneHandler;
  private final ByteBuffer plpData;
  private final Charset charset;

  /**
   * Constructs a new TdsClob.
   *
   * @param transport The transport layer.
   * @param controlPlaneHandler The handler for the control plane.
   * @param plpData The fully extracted raw bytes belonging to this CLOB.
   * @param charset The charset used to decode the CLOB data.
   */
  public TdsClob(
      TdsTransport transport,
      TdsStreamHandler controlPlaneHandler,
      ByteBuffer plpData,
      Charset charset) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.plpData = plpData; // Now holds the fully extracted clean data
    this.charset = charset;
  }

  @Override
  public Publisher<CharSequence> stream() {
    return new Publisher<CharSequence>() {
      @Override
      public void subscribe(Subscriber<? super CharSequence> subscriber) {
        subscriber.onSubscribe(new Subscription() {
          @Override
          public void request(long n) {
            if (n > 0 && plpData != null && plpData.hasRemaining()) {
              // Decode the buffered PLP data into a CharSequence instantly when requested
              CharBuffer charBuffer = charset.decode(plpData.duplicate());
              subscriber.onNext(charBuffer.toString());

              // Mark the underlying buffer as consumed
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
    return null; // Omitted for MVC
  }
}