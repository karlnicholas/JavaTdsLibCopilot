package org.tdslib.javatdslib.codec;

import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.protocol.rpc.ParameterEncoder;
import org.tdslib.javatdslib.protocol.rpc.StreamingParameterEncoder;

import java.util.ArrayList;
import java.util.List;

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

    DEFAULT.registerStreaming(new ClobStreamingEncoder());
  }

  private final List<ParameterEncoder> codecs = new ArrayList<>();
  // Add this to EncoderRegistry.java
  private final List<StreamingParameterEncoder> streamingCodecs = new ArrayList<>();

  /**
   * Registers a new streaming codec.
   *
   * @param codec the streaming codec to register
   */
  public void registerStreaming(StreamingParameterEncoder codec) {
    streamingCodecs.add(codec);
  }

  /**
   * Retrieves a streaming codec capable of encoding the given parameter entry.
   *
   * @param entry the parameter entry to encode
   * @return a suitable StreamingParameterEncoder, or null if none is found.
   */
  public StreamingParameterEncoder getStreamingCodec(TdsParameter entry) {
    for (StreamingParameterEncoder codec : streamingCodecs) {
      if (codec.canEncode(entry)) {
        return codec;
      }
    }
    return null; // Return null to fall back to standard codecs
  }

  /**
   * Registers a new codec.
   *
   * @param codec the codec to register
   */
  public void register(ParameterEncoder codec) {
    codecs.add(codec);
  }

  /**
   * Retrieves a codec capable of encoding the given parameter entry.
   *
   * @param entry the parameter entry to encode
   * @return a suitable ParameterEncoder
   * @throws IllegalArgumentException if no suitable codec is found
   */
  public ParameterEncoder getCodec(TdsParameter entry) {
    for (ParameterEncoder codec : codecs) {
      if (codec.canEncode(entry)) {
        return codec;
      }
    }
    throw new IllegalArgumentException("No codec registered for type: " + entry.type());
  }
}