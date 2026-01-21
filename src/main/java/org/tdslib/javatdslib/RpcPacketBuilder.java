package org.tdslib.javatdslib;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class RpcPacketBuilder {

  private static final byte TYPE_NVARCHAR = (byte) 0xE7;
  private static final byte TYPE_BIGINT   = 0x7F;

  private static final byte RPC_PARAM_INPUT = 0x01;

  public ByteBuffer buildRpcPayload(
          String firstName,
          String lastName,
          String email,
          long postCount) {

    ByteBuffer buf = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);

    // FIXED: ProcID switch + numeric ProcID for sp_executesql
    buf.putShort((short) 0xFFFF);     // ProcIDSwitch = 0xFFFF (signals numeric ID follows)
    buf.putShort((short) 10);         // ProcID = 10 (sp_executesql)

    // RPC flags
    buf.putShort((byte) 0x00);

    // Param 1: @stmt
//    putParamName(buf, "@stmt");
    buf.put((byte) 0);
//    buf.put(RPC_PARAM_INPUT);
    buf.put((byte) 0);
    putTypeInfoNVarcharMax(buf, 8000);
    putPlpUnicodeStringParamValue(buf,
"""
INSERT INTO dbo.users (firstName, lastName, email, postCount) VALUES ( @P0 ,  @P1 ,  @P2 ,  @P3 )
    """);

    // Param 2: @params
//    putParamName(buf, "@params");
    buf.put((byte) 0);
//    buf.put(RPC_PARAM_INPUT);
    buf.put((byte) 0);
    putTypeInfoNVarcharMax(buf, 8000);
    putPlpUnicodeStringParamValue(buf,
            "@P0 nvarchar(4000),@P1 nvarchar(4000),@P2 nvarchar(4000),@P3 bigint");

    // Param 3: @p1 = firstName
    buf.put((byte) 0);
//    putParamName(buf, "@P0");
//    buf.put(RPC_PARAM_INPUT);
    buf.put((byte) 0);
    putTypeInfoNVarchar(buf, 8000);
    putPlpUnicodeStringParamValue(buf, firstName);

    // Param 4: @p2 = lastName
    buf.put((byte) 0);
//    putParamName(buf, "@P1");
//    buf.put(RPC_PARAM_INPUT);
    buf.put((byte) 0);
    putTypeInfoNVarchar(buf, 8000);
    putPlpUnicodeStringParamValue(buf, lastName);

    // Param 5: @p3 = email
    buf.put((byte) 0);
//    putParamName(buf, "@P2");
//    buf.put(RPC_PARAM_INPUT);
    buf.put((byte) 0);
    putTypeInfoNVarchar(buf, 8000);
    putPlpUnicodeStringParamValue(buf, email);

    // Param 6: @p4 = postCount
    buf.put((byte) 0);
//    putParamName(buf, "@P3");
//    buf.put(RPC_PARAM_INPUT);
    buf.put((byte) 0);
    buf.put((byte) 0x26);
    buf.put((byte) 8);
//    buf.put((byte) 0x00);  // no extra info
    buf.put((byte) 8);
    buf.putLong(postCount);

    buf.flip();
    return buf;
  }

  private static void putParamName(ByteBuffer buf, String name) {
    byte[] utf16 = name.getBytes(StandardCharsets.UTF_16LE);
    buf.put((byte) (utf16.length / 2));
    buf.put(utf16);
  }

  private static void putTypeInfoNVarcharMax(ByteBuffer buf, int maxChars) {
    buf.put(TYPE_NVARCHAR);
    buf.putShort((short) maxChars);  // MAX
    putCollation(buf);
  }

  private static void putTypeInfoNVarchar(ByteBuffer buf, int maxChars) {
    buf.put(TYPE_NVARCHAR);
    buf.putShort((short) (maxChars));  // bytes
    putCollation(buf);
  }

  private static void putCollation(ByteBuffer buf) {
    buf.putInt(0x00D00409);   // LCID + version (SQL_Latin1_General_CP1_CI_AS)
    buf.put((byte) 0x34);     // Sort ID (52 = CI_AS)
    // No extra bytes — exactly 5 bytes total for collation
  }

//  private static void putPlpUnicodeString(ByteBuffer buf, String value) {
//    if (value == null) {
//      // TRUE SQL NULL for PLP types: marker + total length -1, NO chunks
//      buf.putInt(-1);                     // 0xFFFFFFFF
//      buf.putLong(-1L);                   // 0xFFFFFFFFFFFFFFFF
//      return;
//    }
//
//    byte[] utf16 = value.getBytes(StandardCharsets.UTF_16LE);
//
//    if (utf16.length == 0) {
//      // Empty string '' → one 0-length chunk
//      buf.putInt(-1);          // PLP marker
//      buf.putInt(0);           // chunk length 0
//      buf.putInt(0);           // end of chunks
//      buf.putLong(-1L);        // total unknown
//      return;
//    }
//
//    // Normal value
////    buf.putLong(-1);                     // PLP marker
////    buf.put((byte) 0);
//    buf.putLong(utf16.length);           // first (and only) chunk
//    buf.putInt(utf16.length);           // first (and only) chunk
//    buf.put(utf16);
//    buf.putInt(0);                      // end
////    buf.putLong(-1L);                   // total unknown
//  }
  private static void putPlpUnicodeStringParamValue(ByteBuffer buf, String value) {
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
//    buf.putLong(-1);                     // PLP marker
//    buf.put((byte) 0);
    buf.putShort((short) utf16.length);           // first (and only) chunk
//    buf.putInt(utf16.length);           // first (and only) chunk
    buf.put(utf16);
//    buf.putInt(0);                      // end
//    buf.putLong(-1L);                   // total unknown
  }
}