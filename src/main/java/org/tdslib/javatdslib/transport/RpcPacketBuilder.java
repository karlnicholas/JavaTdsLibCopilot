package org.tdslib.javatdslib.transport;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.codec.EncoderRegistry;
import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.ParameterEncoder;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;
import org.tdslib.javatdslib.protocol.rpc.StreamingParameterEncoder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Builds TDS RPC packets for executing parameterized queries. */
public class RpcPacketBuilder {
  private static final Logger logger = LoggerFactory.getLogger(RpcPacketBuilder.class);

  private static final short RPC_PROCID_SPEXECUTESQL = 10;
  private static final byte RPC_PARAM_DEFAULT = 0x00;

  // --- New Protocol Constants ---
  // TDS 7.2+ Batch Separator
  private static final byte RPC_BATCH_SEPARATOR = (byte) 0xFF;
  private static final byte RPC_PARAM_BYREF = 0x01;

  // Extracted Magic Numbers
  private static final short RPC_HEADER_MARKER = (short) 0xFFFF;
  private static final short MAX_NVARCHAR_SIZE = 8000;

  private final String sql;
  private final List<List<TdsParameter>> batchParams; // CHANGED from ParamEntry
  private final EncoderRegistry encoderRegistry;
  private final RpcEncodingContext encodingContext;

  /**
   * Creates a new RpcPacketBuilder.
   *
   * @param sql the SQL statement
   * @param batchParams the list of parameter sets for batch execution
   * @param encoderRegistry the registry for parameter codecs
   * @param encodingContext the encoding context
   */
  public RpcPacketBuilder(
      String sql,
      List<List<TdsParameter>> batchParams,
      EncoderRegistry encoderRegistry,
      RpcEncodingContext encodingContext) {
    this.sql = sql;
    this.batchParams = batchParams;
    this.encoderRegistry = encoderRegistry;
    this.encodingContext = encodingContext;
  }

  /**
   * Builds the RPC packet buffer.
   *
   * @return the constructed ByteBuffer
   */
  /**
   * Builds the RPC packet buffer stream.
   *
   * @return A Publisher emitting the constructed ByteBuffer(s)
   */
  public Publisher<ByteBuffer> buildRpcPacket() {
    List<Publisher<ByteBuffer>> segments = new ArrayList<>();

    // TODO: hardcode pipeline length.
    // This buffer now only acts as a temporary scratchpad for synchronous writes.
    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
    buf.order(ByteOrder.LITTLE_ENDIAN);

    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);

    for (int i = 0; i < batchParams.size(); i++) {
      if (i > 0) {
        buf.put(RPC_BATCH_SEPARATOR);
      }

      writeRpcHeader(buf);
      writeFrameworkParamHeader(buf, "@stmt");

      buf.putShort((short) sqlBytes.length);
      buf.put(sqlBytes);

      List<TdsParameter> params = batchParams.get(i);

      if (!params.isEmpty()) {
        writeFrameworkParamHeader(buf, "@params");

        String paramDecl = buildParamDecl(params);
        byte[] declBytes = paramDecl.getBytes(StandardCharsets.UTF_16LE);
        buf.putShort((short) declBytes.length);
        buf.put(declBytes);

        for (TdsParameter param : params) {
          StreamingParameterEncoder streamCodec = encoderRegistry.getStreamingCodec(param);

          if (streamCodec != null) {
            // 1. Write the parameter metadata synchronously
            writeParamName(buf, param.name());
            buf.put(param.isOutParameter() ? RPC_PARAM_BYREF : RPC_PARAM_DEFAULT);
            streamCodec.writeTypeInfo(buf, param, encodingContext);

            // 2. Flush the synchronous buffer to the segment list
            flushBuffer(segments, buf);

            // 3. Append the reactive LOB payload stream directly
            segments.add(streamCodec.streamValue(param, encodingContext));

          } else {
            // Standard scalar parameter - write fully to the synchronous buffer
            writeParam(buf, param);
          }
        }
      }
    }

    // Flush any remaining synchronous bytes at the end of the batch
    flushBuffer(segments, buf);

    // Stitch the entire reactive pipeline together into a contiguous stream
    return Flux.concat(segments);
  }

//  public Publisher<ByteBuffer> buildRpcPacket() {
//    return Mono.fromSupplier(() -> {
//      // TODO: hardcode pipeline length
//      ByteBuffer buf = ByteBuffer.allocate(1024 * 1024); // Large buffer for pipelining
//      buf.order(ByteOrder.LITTLE_ENDIAN);
//
//      byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
//
//      for (int i = 0; i < batchParams.size(); i++) {
//        if (i > 0) {
//          buf.put(RPC_BATCH_SEPARATOR);
//        }
//
//        writeRpcHeader(buf);
//        writeFrameworkParamHeader(buf, "@stmt");
//
//        buf.putShort((short) sqlBytes.length);
//        buf.put(sqlBytes);
//
//        List<TdsParameter> params = batchParams.get(i);
//
//        if (!params.isEmpty()) {
//          writeFrameworkParamHeader(buf, "@params");
//
//          String paramDecl = buildParamDecl(params);
//          byte[] declBytes = paramDecl.getBytes(StandardCharsets.UTF_16LE);
//          buf.putShort((short) declBytes.length);
//          buf.put(declBytes);
//
//          for (TdsParameter param : params) {
//            writeParam(buf, param);
//          }
//        }
//      }
//
//      buf.flip();
//      return buf;
//    });
//  }
//  public ByteBuffer buildRpcPacket() {
//    // TODO: hardcode pipeline length
//    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024); // Large buffer for pipelining
//    buf.order(ByteOrder.LITTLE_ENDIAN);
//
//    // Hoisted Loop Invariant: Encode the SQL string exactly once
//    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
//
//    for (int i = 0; i < batchParams.size(); i++) {
//      // Separates multiple RPCReqBatch requests in TDS 7.2+
//      if (i > 0) {
//        buf.put(RPC_BATCH_SEPARATOR);
//      }
//
//      writeRpcHeader(buf);
//
//      // 1. Framework @stmt header (Hardcoded as nvarchar for protocol framing)
//      writeFrameworkParamHeader(buf, "@stmt");
//
//      buf.putShort((short) sqlBytes.length);
//      buf.put(sqlBytes);
//
//      List<TdsParameter> params = batchParams.get(i);
//
//      // 2. Framework @params header
//      if (!params.isEmpty()) {
//        writeFrameworkParamHeader(buf, "@params");
//
//        String paramDecl = buildParamDecl(params);
//        byte[] declBytes = paramDecl.getBytes(StandardCharsets.UTF_16LE);
//        buf.putShort((short) declBytes.length);
//        buf.put(declBytes);
//
//        // 3. User Values (Delegated to EncoderRegistry)
//        for (TdsParameter param : params) {
//          writeParam(buf, param);
//        }
//      }
//    }
//
//    buf.flip();
//    return buf;
//  }

  private void writeRpcHeader(ByteBuffer buf) {
    buf.putShort(RPC_HEADER_MARKER);
    buf.putShort(RPC_PROCID_SPEXECUTESQL);
    buf.putShort((short) 0);
  }

  /** DRY extraction for writing framework parameter headers (@stmt and @params). */
  private void writeFrameworkParamHeader(ByteBuffer buf, String paramName) {
    writeParamName(buf, paramName);
    buf.put(RPC_PARAM_DEFAULT);
    buf.put((byte) TdsType.NVARCHAR.byteVal);
    buf.putShort(MAX_NVARCHAR_SIZE);
    writeFrameworkCollation(buf);
  }

//  private String buildParamDecl(List<TdsParameter> params) {
//    if (params.isEmpty()) {
//      return "";
//    }
//    StringBuilder sb = new StringBuilder();
//    for (int i = 0; i < params.size(); i++) {
//      TdsParameter p = params.get(i);
//      if (i > 0) {
//        sb.append(",");
//      }
//
//      // Fix: Pass 'p' directly instead of 'p.type()'
//      ParameterEncoder codec = encoderRegistry.getCodec(p);
//      String decl = codec.getSqlTypeDeclaration(p);
//
//      sb.append(p.name()).append(" ").append(decl);
//      if (p.isOutParameter()) {
//        sb.append(" output");
//      }
//    }
//    return sb.toString();
//  }

  private String buildParamDecl(List<TdsParameter> params) {
    if (params.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < params.size(); i++) {
      TdsParameter p = params.get(i);
      if (i > 0) {
        sb.append(",");
      }

      String decl;
      StreamingParameterEncoder streamCodec = encoderRegistry.getStreamingCodec(p);

      if (streamCodec != null) {
        decl = streamCodec.getSqlTypeDeclaration(p);
      } else {
        ParameterEncoder codec = encoderRegistry.getCodec(p);
        decl = codec.getSqlTypeDeclaration(p);
      }

      sb.append(p.name()).append(" ").append(decl);
      if (p.isOutParameter()) {
        sb.append(" output");
      }
    }
    return sb.toString();
  }

  /** Flushes the current synchronous buffer into the reactive segment list. */
  private void flushBuffer(List<Publisher<ByteBuffer>> segments, ByteBuffer buf) {
    buf.flip();
    if (buf.hasRemaining()) {
      // Create an exactly-sized copy of the active bytes
      ByteBuffer chunk = ByteBuffer.allocate(buf.remaining()).order(ByteOrder.LITTLE_ENDIAN);
      chunk.put(buf);
      chunk.flip();
      segments.add(Mono.just(chunk));
    }

    // Reset the buffer for the next round of synchronous writing
    buf.clear();
    buf.order(ByteOrder.LITTLE_ENDIAN);
  }

  private void writeParam(ByteBuffer buf, TdsParameter param) {
    writeParamName(buf, param.name());

    if (param.isOutParameter()) {
      buf.put(RPC_PARAM_BYREF);
    } else {
      buf.put(RPC_PARAM_DEFAULT);
    }

    ParameterEncoder codec = encoderRegistry.getCodec(param);
    codec.writeTypeInfo(buf, param, encodingContext);
    codec.writeValue(buf, param, encodingContext);
  }

  private void writeParamName(ByteBuffer buf, String name) {
    if (name == null || name.isEmpty()) {
      buf.put((byte) 0);
      return;
    }
    buf.put((byte) name.length());
    buf.put(name.getBytes(StandardCharsets.UTF_16LE));
  }

  private void writeFrameworkCollation(ByteBuffer buf) {
    buf.put(encodingContext.collationBytes());
  }
}
