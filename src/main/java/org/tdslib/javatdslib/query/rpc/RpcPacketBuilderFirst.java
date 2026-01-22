package org.tdslib.javatdslib.query.rpc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class RpcPacketBuilderFirst {

  private static final byte TYPE_NVARCHAR = (byte) 0xE7;
  private static final byte TYPE_BIGINT   = 0x7F;

  private static final byte RPC_PARAM_INPUT = 0x01;

  public ByteBuffer buildRpcPayload(
      String firstName,
      String lastName,
      String email,
      long postCount) {

    ByteBuffer buf = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);

    // ProcID = 10 (sp_executesql)
    buf.putShort((short) 10);

    // RPC flags
    buf.put((byte) 0x00);

    // Param 1: @stmt
    putParamName(buf, "@stmt");
    buf.put(RPC_PARAM_INPUT);
    putTypeInfoNVarcharMax(buf);
    putPlpUnicodeString(buf,
        "INSERT INTO dbo.users (firstName, lastName, email, postCount) VALUES (@p1, @p2, @p3, @p4)");

    // Param 2: @params
    putParamName(buf, "@params");
    buf.put(RPC_PARAM_INPUT);
    putTypeInfoNVarcharMax(buf);
    putPlpUnicodeString(buf,
        "@p1 nvarchar(100), @p2 nvarchar(100), @p3 nvarchar(254), @p4 bigint");

    // Param 3: @p1 = firstName
    putParamName(buf, "@p1");
    buf.put(RPC_PARAM_INPUT);
    putTypeInfoNVarchar(buf, 100);
    putPlpUnicodeString(buf, firstName);

    // Param 4: @p2 = lastName
    putParamName(buf, "@p2");
    buf.put(RPC_PARAM_INPUT);
    putTypeInfoNVarchar(buf, 100);
    putPlpUnicodeString(buf, lastName);

    // Param 5: @p3 = email
    putParamName(buf, "@p3");
    buf.put(RPC_PARAM_INPUT);
    putTypeInfoNVarchar(buf, 254);
    putPlpUnicodeString(buf, email);

    // Param 6: @p4 = postCount
    putParamName(buf, "@p4");
    buf.put(RPC_PARAM_INPUT);
    buf.put(TYPE_BIGINT);
    buf.put((byte) 0x00);  // no extra info
    buf.putLong(postCount);

    buf.flip();
    return buf;
  }

  private static void putParamName(ByteBuffer buf, String name) {
    byte[] utf16 = name.getBytes(StandardCharsets.UTF_16LE);
    buf.putShort((short) (utf16.length / 2));
    buf.put(utf16);
  }

  private static void putTypeInfoNVarcharMax(ByteBuffer buf) {
    buf.put(TYPE_NVARCHAR);
    buf.putShort((short) 0xFFFF);  // MAX
    putCollation(buf);
  }

  private static void putTypeInfoNVarchar(ByteBuffer buf, int maxChars) {
    buf.put(TYPE_NVARCHAR);
    buf.putShort((short) (maxChars * 2));  // bytes
    putCollation(buf);
  }

  private static void putCollation(ByteBuffer buf) {
    buf.putInt(0x00000409);   // LCID + version (SQL_Latin1_General_CP1_CI_AS)
    buf.put((byte) 0x34);     // Sort ID (52 = CI_AS)
    // No extra bytes — exactly 5 bytes total for collation
  }

  private static void putPlpUnicodeString(ByteBuffer buf, String value) {
    if (value == null) {
      // TRUE SQL NULL for PLP types: marker + total length -1, NO chunks
      buf.putInt(-1);                     // 0xFFFFFFFF
      buf.putLong(-1L);                   // 0xFFFFFFFFFFFFFFFF
      return;
    }

    byte[] utf16 = value.getBytes(StandardCharsets.UTF_16LE);

    if (utf16.length == 0) {
      // Empty string '' → one 0-length chunk
      buf.putInt(-1);          // PLP marker
      buf.putInt(0);           // chunk length 0
      buf.putInt(0);           // end of chunks
      buf.putLong(-1L);        // total unknown
      return;
    }

    // Normal value
    buf.putInt(-1);                     // PLP marker
    buf.putInt(utf16.length);           // first (and only) chunk
    buf.put(utf16);
    buf.putInt(0);                      // end
    buf.putLong(-1L);                   // total unknown
  }
}