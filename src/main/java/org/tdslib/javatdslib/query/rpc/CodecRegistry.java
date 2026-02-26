package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.query.rpc.codecs.BigDecimalCodec;
import org.tdslib.javatdslib.query.rpc.codecs.BinaryCodec;
import org.tdslib.javatdslib.query.rpc.codecs.BooleanCodec;
import org.tdslib.javatdslib.query.rpc.codecs.DateTimeCodec;
import org.tdslib.javatdslib.query.rpc.codecs.FloatCodec;
import org.tdslib.javatdslib.query.rpc.codecs.GuidCodec;
import org.tdslib.javatdslib.query.rpc.codecs.IntegerCodec;
import org.tdslib.javatdslib.query.rpc.codecs.StringCodec;

import java.util.ArrayList;
import java.util.List;

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

  public void register(ParameterCodec codec) {
    codecs.add(codec);
  }

  public ParameterCodec getCodec(ParamEntry entry) {
    for (ParameterCodec codec : codecs) {
      if (codec.canEncode(entry)) {
        return codec;
      }
    }
    throw new IllegalArgumentException("No codec registered for type: " + entry.key().type());
  }
}