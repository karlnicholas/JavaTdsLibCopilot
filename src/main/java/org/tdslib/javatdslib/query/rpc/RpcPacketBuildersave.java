package org.tdslib.javatdslib.query.rpc;

import org.tdslib.javatdslib.headers.AllHeaders;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class RpcPacketBuildersave {

  private static final byte TYPE_NVARCHAR = (byte) 0xE7;
  private static final byte TYPE_BIGINT   = 0x7F;

  private static final byte RPC_PARAM_DEFAULT = 0x00; // Standard Input parameter.
  private static final byte RPC_PARAM_BYREF = 0x01; // Output parameter (passed by reference).
  private static final byte RPC_PARAM_DEFAULT_VALUE = 0x02; // Use the procedure's default value (the value sent is ignored).

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
    putParamName(buf, "@stmt");
//    buf.put((byte) 0);
    buf.put(RPC_PARAM_DEFAULT);
    putTypeInfoNVarcharMax(buf, 4000);
    putPlpUnicodeString(buf,
        "INSERT INTO dbo.users (firstName, lastName, email, postCount) VALUES (@p1, @p2, @p3, @p4)");

    // Param 2: @params
    putParamName(buf, "@params");
//    buf.put((byte) 0);
    buf.put(RPC_PARAM_DEFAULT);
    putTypeInfoNVarcharMax(buf, 4000);
    putPlpUnicodeString(buf,
        "@p1 nvarchar(100), @p2 nvarchar(100), @p3 nvarchar(254), @p4 bigint");

    // Param 3: @p1 = firstName
//    buf.put((byte) 0);
    putParamName(buf, "@p1");
    buf.put(RPC_PARAM_DEFAULT);
    putTypeInfoNVarchar(buf, 100);
    putPlpUnicodeString(buf, firstName);

    // Param 4: @p2 = lastName
//    buf.put((byte) 0);
    putParamName(buf, "@p2");
    buf.put(RPC_PARAM_DEFAULT);
    putTypeInfoNVarchar(buf, 100);
    putPlpUnicodeString(buf, lastName);

    // Param 5: @p3 = email
//    buf.put((byte) 0);
    putParamName(buf, "@p3");
    buf.put(RPC_PARAM_DEFAULT);
    putTypeInfoNVarchar(buf, 254);
    putPlpUnicodeString(buf, email);

    // Param 6: @p4 = postCount
//    buf.put((byte) 0);
    putParamName(buf, "@p4");
    buf.put(RPC_PARAM_DEFAULT);
    buf.put((byte) 0x26);
    buf.put((byte) 8);
//    buf.put((byte) 0x00);  // no extra info
    buf.put((byte) 8);
    buf.putLong(postCount);

//    // Param 6: @p4 = postCount
//    putParamName(buf, "@p4");
//    buf.put(RPC_PARAM_DEFAULT);
//    buf.put(TYPE_BIGINT);
////    buf.put((byte) 0x00);  // no extra info
//    buf.putLong(postCount);


    buf.flip();
// Now build ALL_HEADERS (most common: auto-commit, transaction=0, outstanding=1)
    byte[] allHeadersBytes = AllHeaders.forAutoCommit(1).toBytes();

    // Combine: ALL_HEADERS + RPC core
    ByteBuffer fullPayload = ByteBuffer.allocate(allHeadersBytes.length + buf.limit())
        .order(ByteOrder.LITTLE_ENDIAN);
    fullPayload.put(allHeadersBytes);
    fullPayload.put(buf);

    fullPayload.flip();
    return fullPayload;
//    return buf;
  }

//  JDBC Method	Generated SQL Type in @params	TDS Token used for Value
//  setShort()	smallint	0x26 (Length 2)
//  setInt()	int	0x26 (Length 4)
//  setLong()	bigint	0x26 (Length 8)
//  setString()	nvarchar(4000) or (max)	0xe7
//  setBytes()	varbinary(8000) or (max)	0xa5
//  setBoolean()	bit	0x32
//

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
////    buf.putInt(-1);                     // PLP marker
//    buf.putInt(utf16.length);           // first (and only) chunk
//    buf.put(utf16);
//    buf.putInt(0);                      // end
////    buf.putLong(-1L);                   // total unknown
//  }

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
//    buf.putLong(-1);                     // PLP marker
//    buf.put((byte) 0);
    buf.putShort((short) utf16.length);           // first (and only) chunk
//    buf.putInt(utf16.length);           // first (and only) chunk
    buf.put(utf16);
//    buf.putInt(0);                      // end
//    buf.putLong(-1L);                   // total unknown
  }
}