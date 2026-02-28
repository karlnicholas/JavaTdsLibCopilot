package org.tdslib.javatdslib.decode;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.tdslib.javatdslib.TdsType;

/**
 * Registry for managing and accessing ResultDecoders.
 */
public class DecoderRegistry {
  public static final DecoderRegistry DEFAULT = new DecoderRegistry();

  private final List<ResultDecoder> decoders = new ArrayList<>();

  static {
    DEFAULT.register(new StringDecoder());
    DEFAULT.register(new NumericDecoder());
    DEFAULT.register(new DateTimeDecoder());
    DEFAULT.register(new BinaryDecoder());
    DEFAULT.register(new GuidDecoder()); // Added GuidDecoder
  }

  /**
   * Registers a new decoder.
   *
   * @param decoder the decoder to register
   */
  public void register(ResultDecoder decoder) {
    decoders.add(decoder);
  }

  /**
   * Decodes the given data using a registered decoder.
   */
  public <T> T decode(byte[] data, TdsType tdsType, Class<T> targetType, int scale,
                      Charset varcharCharset) {
    if (data == null) {
      return null;
    }

    for (ResultDecoder decoder : decoders) {
      if (decoder.canDecode(tdsType)) {
        return decoder.decode(data, tdsType, targetType, scale, varcharCharset);
      }
    }
    throw new UnsupportedOperationException("No decoder registered for TDS type: " + tdsType);
  }
}