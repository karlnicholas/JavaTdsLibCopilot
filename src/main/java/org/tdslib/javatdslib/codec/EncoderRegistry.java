package org.tdslib.javatdslib.codec;

import java.util.ArrayList;
import java.util.List;

import org.tdslib.javatdslib.protocol.rpc.ParamEntry;
import org.tdslib.javatdslib.protocol.rpc.ParameterCodec;

/**
 * Registry for managing and accessing ParameterCodecs.
 */
public class EncoderRegistry {

  public static final EncoderRegistry DEFAULT = new EncoderRegistry();

  static {
    DEFAULT.register(new IntegerEncoder());
    DEFAULT.register(new StringEncoder());
    DEFAULT.register(new BigDecimalEncoder());
    DEFAULT.register(new BooleanEncoder());
    DEFAULT.register(new FloatEncoder());
    DEFAULT.register(new DateTimeEncoder());
    DEFAULT.register(new BinaryEncoder());
    DEFAULT.register(new GuidEncoder());
  }

  private final List<ParameterCodec> codecs = new ArrayList<>();

  /**
   * Registers a new codec.
   *
   * @param codec the codec to register
   */
  public void register(ParameterCodec codec) {
    codecs.add(codec);
  }

  /**
   * Retrieves a codec capable of encoding the given parameter entry.
   *
   * @param entry the parameter entry to encode
   * @return a suitable ParameterCodec
   * @throws IllegalArgumentException if no suitable codec is found
   */
  public ParameterCodec getCodec(ParamEntry entry) {
    for (ParameterCodec codec : codecs) {
      if (codec.canEncode(entry)) {
        return codec;
      }
    }
    throw new IllegalArgumentException("No codec registered for type: " + entry.key().type());
  }
}