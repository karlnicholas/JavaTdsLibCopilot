package org.tdslib.javatdslib.query.rpc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RpcPacketBuilder {

  private static final short RPC_PROCID_SWITCH = (short) 0xFFFF;
  private static final short RPC_PROCID_SPEXECUTESQL = 10;

  private static final byte RPC_PARAM_DEFAULT = 0x00;

  private final String sql;
  private final List<ParamEntry> params;

  public RpcPacketBuilder(String sql, List<ParamEntry> params) {
    this.sql = sql;
    this.params = params;
  }

  public ByteBuffer buildRpcPacket() {
    ByteBuffer buf = ByteBuffer.allocate(8192).order(ByteOrder.LITTLE_ENDIAN);

    // RPC header
    buf.putShort(RPC_PROCID_SWITCH);
    buf.putShort(RPC_PROCID_SPEXECUTESQL);
    buf.putShort((short) 0x0000);

    // Param 1: @stmt
    putParamName(buf, "@stmt");
    buf.put(RPC_PARAM_DEFAULT);
    putTypeInfoNVarcharMax(buf);
    putPlpUnicodeString(buf, sql);

    // Param 2: @params — entry for every parameter
    String paramsDecl = buildParamsDeclaration();
    putParamName(buf, "@params");
    buf.put(RPC_PARAM_DEFAULT);
    putTypeInfoNVarcharMax(buf);
    putPlpUnicodeString(buf, paramsDecl);

    // All parameters — use real name or empty
    for (int i = 0; i < params.size(); i++) {
      ParamEntry entry = params.get(i);
      String rpcParamName = getRpcParamName(entry);           // real or empty

      putParamName(buf, rpcParamName);                        // empty for unnamed
      buf.put(RPC_PARAM_DEFAULT);
      putTypeInfoForParam(buf, entry);
      putParamValue(buf, entry);
    }

    buf.flip();
    return buf;
  }

  private String buildParamsDeclaration() {
    if (params.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < params.size(); i++) {
      ParamEntry entry = params.get(i);
      String declName = getDeclarationName(entry, i);
      String typeDecl = getSqlTypeDeclaration(entry.bindingType());

      if (i > 0) sb.append(", ");
      sb.append(declName).append(" ").append(typeDecl);
    }
    return sb.toString();
  }

  // Name used in @params declaration — always non-empty
  private String getDeclarationName(ParamEntry entry, int index) {
    String name = entry.key().name();
    if (name != null && !name.isEmpty()) {
      return name.startsWith("@") ? name : "@" + name;
    }
    return "@p" + index;  // dummy for unnamed in declaration
  }

  // Name used in actual RPC parameter token — empty for unnamed
  private String getRpcParamName(ParamEntry entry) {
    String name = entry.key().name();
    if (name != null && !name.isEmpty()) {
      return name.startsWith("@") ? name : "@" + name;
    }
    return "";  // truly unnamed in TDS packet
  }

  private String getSqlTypeDeclaration(BindingType bt) {
    return switch (bt) {
      case SHORT      -> "smallint";
      case INTEGER    -> "int";
      case LONG       -> "bigint";
      case BYTE       -> "tinyint";
      case BOOLEAN    -> "bit";
      case FLOAT      -> "real";
      case DOUBLE     -> "float";
      case BIGDECIMAL -> "decimal(38,10)";
      case STRING     -> "nvarchar(4000)";
      case BYTES      -> "varbinary(8000)";
      case DATE       -> "date";
      case TIME       -> "time(7)";
      case TIMESTAMP  -> "datetime2(7)";
      case CLOB       -> "varchar(max)";
      case NCLOB      -> "nvarchar(max)";
      case BLOB       -> "varbinary(max)";
      case SQLXML     -> "xml";
    };
  }

  // Helpers (unchanged)
  private void putParamName(ByteBuffer buf, String name) {
    byte[] bytes = name.getBytes(StandardCharsets.UTF_16LE);
    buf.putShort((short) (bytes.length / 2));
    buf.put(bytes);
  }

  private void putTypeInfoNVarcharMax(ByteBuffer buf) {
    buf.put((byte) 0xE7);
    buf.putShort((short) 0xFFFF);
    buf.putInt(0x00000409);
    buf.put((byte) 0x00);
    buf.putShort((short) 52);
  }

  private void putPlpUnicodeString(ByteBuffer buf, String str) {
    if (str == null || str.isEmpty()) {
      buf.putInt(-1);
      return;
    }
    byte[] bytes = str.getBytes(StandardCharsets.UTF_16LE);
    buf.putInt(-1);
    buf.putInt(bytes.length);
    buf.put(bytes);
    buf.putInt(0);
  }

  private void putTypeInfoForParam(ByteBuffer buf, ParamEntry entry) {
    byte xtype = entry.getEffectiveXtype();
    buf.put(xtype);

    // Placeholder
    if (entry.bindingType() == BindingType.STRING ||
            entry.bindingType() == BindingType.NCLOB ||
            entry.bindingType() == BindingType.CLOB) {
      buf.putShort((short) 0xFFFF);
    } else if (entry.bindingType() == BindingType.BYTES ||
            entry.bindingType() == BindingType.BLOB) {
      buf.putShort((short) 0xFFFF);
    } else if (entry.bindingType() == BindingType.SQLXML) {
      buf.put((byte) 0x00);
    }
  }

  private void putParamValue(ByteBuffer buf, ParamEntry entry) {
    if (entry.isNull()) {
      buf.putInt(-1);
      return;
    }
    buf.put((byte) 0x00); // placeholder
  }
}