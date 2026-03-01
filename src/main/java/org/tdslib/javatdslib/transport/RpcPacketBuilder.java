package org.tdslib.javatdslib.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.codec.EncoderRegistry;
import org.tdslib.javatdslib.headers.AllHeaders;
import org.tdslib.javatdslib.protocol.TdsParameter;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.protocol.rpc.ParameterEncoder;
import org.tdslib.javatdslib.protocol.rpc.RpcEncodingContext;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Builds TDS RPC packets for executing parameterized queries.
 */
public class RpcPacketBuilder {
  private static final Logger logger = LoggerFactory.getLogger(RpcPacketBuilder.class);

  private static final short RPC_PROCID_SPEXECUTESQL = 10;
  private static final byte RPC_PARAM_DEFAULT = 0x00;

  // Extracted Magic Numbers
  private static final short RPC_HEADER_MARKER = (short) 0xFFFF;
  private static final short MAX_NVARCHAR_SIZE = 8000;

  private final String sql;
  private final List<List<TdsParameter>> batchParams; // CHANGED from ParamEntry
  private final boolean update;
  private final EncoderRegistry encoderRegistry;
  private final RpcEncodingContext encodingContext;

  /**
   * Creates a new RpcPacketBuilder.
   *
   * @param sql             the SQL statement
   * @param batchParams     the list of parameter sets for batch execution
   * @param update          whether this is an update operation
   * @param encoderRegistry   the registry for parameter codecs
   * @param encodingContext the encoding context
   */
  public RpcPacketBuilder(String sql, List<List<TdsParameter>> batchParams, boolean update,
                          EncoderRegistry encoderRegistry, RpcEncodingContext encodingContext) {
    this.sql = sql;
    this.batchParams = batchParams;
    this.update = update;
    this.encoderRegistry = encoderRegistry;
    this.encodingContext = encodingContext;
  }

  /**
   * Builds the RPC packet buffer.
   *
   * @return the constructed ByteBuffer
   */
  public ByteBuffer buildRpcPacket() {
    ByteBuffer buf = ByteBuffer.allocate(1024 * 1024); // Large buffer for pipelining
    buf.order(ByteOrder.LITTLE_ENDIAN);

    // Hoisted Loop Invariant: Encode the SQL string exactly once
    byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);

    for (int i = 0; i < batchParams.size(); i++) {
      // TDS Spec: 0x80 (BatchFlag) separates multiple RPCReqBatch requests
      if (i > 0) {
        buf.put((byte) 0xFF);
      }

      writeRpcHeader(buf);

      // 1. Framework @stmt header (Hardcoded as nvarchar for protocol framing)
      writeFrameworkParamHeader(buf, "@stmt");

      buf.putShort((short) sqlBytes.length);
      buf.put(sqlBytes);

      List<TdsParameter> params = batchParams.get(i);

      // 2. Framework @params header
      if (!params.isEmpty()) {
        writeFrameworkParamHeader(buf, "@params");

        String paramDecl = buildParamDecl(params);
        byte[] declBytes = paramDecl.getBytes(StandardCharsets.UTF_16LE);
        buf.putShort((short) declBytes.length);
        buf.put(declBytes);

        // 3. User Values (Delegated to EncoderRegistry)
        for (TdsParameter param : params) {
          writeParam(buf, param);
        }
      }
    }

    buf.flip();
    if (update) {
      byte[] allHeadersBytes = AllHeaders.forAutoCommit(1).toBytes();
      ByteBuffer fullPayload = ByteBuffer.allocate(allHeadersBytes.length + buf.limit())
          .order(ByteOrder.LITTLE_ENDIAN);
      fullPayload.put(allHeadersBytes);
      fullPayload.put(buf);
      fullPayload.flip();
      return fullPayload;
    }
    return buf;
  }

  private void writeRpcHeader(ByteBuffer buf) {
    buf.putShort(RPC_HEADER_MARKER);
    buf.putShort(RPC_PROCID_SPEXECUTESQL);
    buf.putShort((short) 0);
  }

  /**
   * DRY extraction for writing framework parameter headers (@stmt and @params).
   */
  private void writeFrameworkParamHeader(ByteBuffer buf, String paramName) {
    writeParamName(buf, paramName);
    buf.put(RPC_PARAM_DEFAULT);
    buf.put((byte) TdsType.NVARCHAR.byteVal);
    buf.putShort(MAX_NVARCHAR_SIZE);
    writeFrameworkCollation(buf);
  }

  private String buildParamDecl(List<TdsParameter> params) {
    if (params.isEmpty()) return "";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < params.size(); i++) {
      TdsParameter p = params.get(i);
      if (i > 0) sb.append(",");

      // Fix: Pass 'p' directly instead of 'p.type()'
      ParameterEncoder codec = encoderRegistry.getCodec(p);
      String decl = codec.getSqlTypeDeclaration(p);

      sb.append(p.name()).append(" ").append(decl);
      if (p.isOutParameter()) {
        sb.append(" output");
      }
    }
    return sb.toString();
  }

  private void writeParam(ByteBuffer buf, TdsParameter param) {
    writeParamName(buf, param.name());

    if (param.isOutParameter()) {
      buf.put((byte) 0x01);
    } else {
      buf.put(RPC_PARAM_DEFAULT);
    }

    // Fix: Pass 'param' directly instead of 'param.type()'
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