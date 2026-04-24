package org.tdslib.javatdslib.codec;

import io.r2dbc.spi.Clob;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;
import org.tdslib.javatdslib.protocol.rpc.StreamingParameterEncoder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class ClobStreamingEncoder implements StreamingParameterEncoder {

  private static final Logger logger = LoggerFactory.getLogger(ClobStreamingEncoder.class);

  @Override
  public boolean canEncode(TdsParameter entry) {
    return entry.value() instanceof Clob;
  }

  @Override
  public String getSqlTypeDeclaration(TdsParameter entry) {
    return "nvarchar(max)";
  }

  @Override
  public void writeTypeInfo(ByteBuffer buf, TdsParameter entry, RpcEncodingContext context) {
    logger.trace("[ClobStreamingEncoder] Writing TypeInfo for nvarchar(max) PLP");
    buf.put((byte) TdsType.NVARCHAR.byteVal);
    buf.putShort((short) -1); // 0xFFFF signals a max-length/PLP type
    buf.put(context.collationBytes());
  }

  @Override
  public Publisher<ByteBuffer> streamValue(TdsParameter entry, RpcEncodingContext context) {
    logger.trace("[ClobStreamingEncoder] Initiating reactive streamValue mapping for Clob");
    Clob clob = (Clob) entry.value();

    // 1. PLP Header: 0xFFFFFFFFFFFFFFFE (-2L) signals an unknown total length
    ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    header.putLong(0xFFFFFFFFFFFFFFFEL);
    header.flip();

    // 2. Map the incoming CharSequence chunks to PLP Data Chunks
    Flux<ByteBuffer> chunkStream = Flux.from(clob.stream())
        .map(charSequence -> {
          byte[] stringBytes = charSequence.toString().getBytes(StandardCharsets.UTF_16LE);

          ByteBuffer chunk = ByteBuffer.allocate(4 + stringBytes.length).order(ByteOrder.LITTLE_ENDIAN);
          chunk.putInt(stringBytes.length);
          chunk.put(stringBytes);
          chunk.flip();

          return chunk;
        })
        .doOnNext(b -> logger.trace("[ClobStreamingEncoder] Emitting PLP Chunk buffer (Total Size: {} bytes)", b.remaining()));

    // 3. Assemble the pipeline: Header -> Chunks -> Terminator
    return Flux.concat(
        Mono.just(header).doOnNext(b -> logger.trace("[ClobStreamingEncoder] Emitting PLP Header: 0xFFFFFFFFFFFFFFFE ({} bytes)", b.remaining())),
        chunkStream,
        Mono.just(createPlpTerminator()).doOnNext(b -> logger.trace("[ClobStreamingEncoder] Emitting PLP Terminator: 0x00000000 ({} bytes)", b.remaining()))
    ).doOnComplete(() -> logger.trace("[ClobStreamingEncoder] PLP stream completely emitted to transport."));
  }

  private ByteBuffer createPlpTerminator() {
    ByteBuffer term = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    term.putInt(0);
    term.flip();
    return term;
  }
}