package org.tdslib.javatdslib.query.rpc;

import io.r2dbc.spi.Parameter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.TdsType;
import org.tdslib.javatdslib.headers.AllHeaders;

/**
 * Builds TDS RPC packets for executing parameterized queries.
 */
public class RpcPacketBuilder {
  private static final Logger logger = LoggerFactory.getLogger(RpcPacketBuilder.class);

  private static final short RPC_PROCID_SPEXECUTESQL = 10;
  private static final byte RPC_PARAM_DEFAULT = 0x00;

  private final String sql;
  private final List<List<ParamEntry>> batchParams;
  private final boolean update;
  private final CodecRegistry codecRegistry;
  private final RpcEncodingContext encodingContext;

  /**
   * Creates a new RpcPacketBuilder.
   *
   * @param sql             the SQL statement
   * @param batchParams     the list of parameter sets for batch execution
   * @param update          whether this is an update operation
   * @param codecRegistry   the registry for parameter codecs
   * @param encodingContext the encoding context
   */
  public RpcPacketBuilder(String sql, List<List<ParamEntry>> batchParams, boolean update,
                          CodecRegistry codecRegistry, RpcEncodingContext encodingContext) {
    this.sql = sql;
    this.batchParams = batchParams;
    this.update = update;
    this.codecRegistry = codecRegistry;
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

    for (int i = 0; i < batchParams.size(); i++) {
      // TDS Spec: 0x80 (BatchFlag) separates multiple RPCReqBatch requests
      if (i > 0) {
        buf.put((byte) 0xFF);
      }

      writeRpcHeader(buf);

      // 1. Framework @stmt header (Hardcoded as nvarchar for protocol framing)
      writeParamName(buf, "@stmt");
      buf.put(RPC_PARAM_DEFAULT);
      buf.put((byte) TdsType.NVARCHAR.byteVal);
      buf.putShort((short) 8000);
      writeFrameworkCollation(buf);

      byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_16LE);
      buf.putShort((short) sqlBytes.length);
      buf.put(sqlBytes);

      List<ParamEntry> params = batchParams.get(i);

      // 2. Framework @params header
      if (!params.isEmpty()) {
        writeParamName(buf, "@params");
        buf.put(RPC_PARAM_DEFAULT);
        buf.put((byte) TdsType.NVARCHAR.byteVal);
        buf.putShort((short) 8000);
        writeFrameworkCollation(buf);

        String paramDecl = buildParamDecl(params);
        byte[] declBytes = paramDecl.getBytes(StandardCharsets.UTF_16LE);
        buf.putShort((short) declBytes.length);
        buf.put(declBytes);

        // 3. User Values (Delegated to CodecRegistry)
        for (ParamEntry param : params) {
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
    buf.putShort((short) 0xFFFF);
    buf.putShort(RPC_PROCID_SPEXECUTESQL);
    buf.putShort((short) 0);
  }

  private String buildParamDecl(List<ParamEntry> params) {
    if (params.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < params.size(); i++) {
      ParamEntry entry = params.get(i);
      if (i > 0) {
        sb.append(",");
      }

      ParameterCodec codec = codecRegistry.getCodec(entry);
      String decl = codec.getSqlTypeDeclaration(entry);

      sb.append(entry.key().name()).append(" ").append(decl);
      if (entry.value() instanceof Parameter.Out) {
        sb.append(" output");
      }
    }
    return sb.toString();
  }

  private void writeParam(ByteBuffer buf, ParamEntry param) {
    writeParamName(buf, param.key().name());

    if (param.value() instanceof Parameter.Out) {
      buf.put((byte) 0x01);
    } else {
      buf.put(RPC_PARAM_DEFAULT);
    }

    ParameterCodec codec = codecRegistry.getCodec(param);
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