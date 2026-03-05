package org.tdslib.javatdslib.streaming;

import io.r2dbc.spi.Clob;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import org.tdslib.javatdslib.transport.TdsStreamHandler;
import org.tdslib.javatdslib.transport.TdsTransport;

/**
 * Implementation of {@link Clob} for the TDS protocol. This class represents a Character Large
 * Object (CLOB) that can be streamed from the database. It handles the streaming of character data
 * chunks using the underlying TDS transport and a specified charset.
 */
public class TdsClob implements Clob {

  private final TdsTransport transport;
  private final TdsStreamHandler controlPlaneHandler;
  private final ByteBuffer leftoverBytes;

  private final java.nio.charset.Charset charset;

  /**
   * Constructs a new TdsClob.
   *
   * @param transport The transport layer for reading CLOB data.
   * @param controlPlaneHandler The handler to switch back to after the CLOB is fully read.
   * @param leftoverBytes Any bytes already read from the stream that belong to this CLOB.
   * @param charset The charset used to decode the CLOB data.
   */
  public TdsClob(
      TdsTransport transport,
      TdsStreamHandler controlPlaneHandler,
      ByteBuffer leftoverBytes,
      java.nio.charset.Charset charset) {
    this.transport = transport;
    this.controlPlaneHandler = controlPlaneHandler;
    this.leftoverBytes = leftoverBytes;
    this.charset = charset;
  }

  @Override
  public Publisher<CharSequence> stream() {
    return subscriber -> {
      // Pass the charset into the PLP stream handler
      PlpStreamHandler plpHandler =
          new PlpStreamHandler(transport, controlPlaneHandler, subscriber, charset);

      // 2. Hijack the network stream
      transport.setStreamHandlers(plpHandler, null);

      // 3. Feed the leftover bytes FIRST!
      if (leftoverBytes != null && leftoverBytes.hasRemaining()) {
        plpHandler.onPayloadAvailable(leftoverBytes, false);
      }

      // 4. Open the valve for future packets!
      transport.resumeNetworkRead();
    };
  }

  @Override
  public Publisher<Void> discard() {
    return null; // Omitted for MVC
  }
}
