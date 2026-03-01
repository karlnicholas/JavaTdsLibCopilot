package org.tdslib.javatdslib.query.rpc;

import java.util.ArrayList;
import java.util.List;
import org.tdslib.javatdslib.codec.BigDecimalCodec;
import org.tdslib.javatdslib.codec.BinaryCodec;
import org.tdslib.javatdslib.codec.BooleanCodec;
import org.tdslib.javatdslib.codec.DateTimeCodec;
import org.tdslib.javatdslib.codec.FloatCodec;
import org.tdslib.javatdslib.codec.GuidCodec;
import org.tdslib.javatdslib.codec.IntegerCodec;
import org.tdslib.javatdslib.codec.StringCodec;

/**
 * Registry for managing and accessing ParameterCodecs.
 */
public class CodecRegistry {

  public static final CodecRegistry DEFAULT = new CodecRegistry();

  static {
    DEFAULT.register(new IntegerCodec());
    DEFAULT.register(new StringCodec());
    DEFAULT.register(new BigDecimalCodec());
    DEFAULT.register(new BooleanCodec());
    DEFAULT.register(new FloatCodec());
    DEFAULT.register(new DateTimeCodec());
    DEFAULT.register(new BinaryCodec());
    DEFAULT.register(new GuidCodec());
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