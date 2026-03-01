package org.tdslib.javatdslib.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.tdslib.javatdslib.protocol.TdsType;
import org.tdslib.javatdslib.query.rpc.ParamEntry;
import org.tdslib.javatdslib.query.rpc.ParameterCodec;
import org.tdslib.javatdslib.query.rpc.RpcEncodingContext;

/**
 * Codec for encoding String values into TDS CHAR/VARCHAR/NCHAR/NVARCHAR format.
 */
public class StringCodec implements ParameterCodec {

  @Override
  public boolean canEncode(ParamEntry entry) {
    TdsType type = entry.key().type();
    return type == TdsType.NVARCHAR || type == TdsType.NCHAR || type == TdsType.NTEXT
        || type == TdsType.VARCHAR || type == TdsType.CHAR || type == TdsType.TEXT
        || type == TdsType.BIGVARCHR || type == TdsType.BIGCHAR;
  }

  @Override
  public String getSqlTypeDeclaration(ParamEntry entry) {
    TdsType type = entry.key().type();
    boolean isNational = isNationalType(type);
    boolean isLarge = isLargeString(entry);

    if (isNational) {
      return isLarge ? "nvarchar(max)" : "nvarchar(4000)";
    } else {
      return isLarge ? "varchar(max)" : "varchar(8000)";
    }
  }

  @Override
  public void writeTypeInfo(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    TdsType type = entry.key().type();
    buf.put((byte) type.byteVal);

    int encodedLen = getEncodedLength(entry, context);

    if (encodedLen > 8000) {
      buf.putShort((short) -1); // 0xFFFF signals a max-length/PLP type
    } else {
      // In RPC TYPE_INFO, we send the maximum capacity of the declared type,
      // not the string's actual length
      buf.putShort((short) 8000);
    }

    // All character types require the 5-byte collation payload
    buf.put(context.collationBytes());
  }

  @Override
  public void writeValue(ByteBuffer buf, ParamEntry entry, RpcEncodingContext context) {
    Object value = entry.value().getValue();
    if (value == null) {
      writeNull(buf, entry.key().type());
      return;
    }

    String stringVal = (value instanceof String) ? (String) value : value.toString();
    TdsType type = entry.key().type();
    boolean isNational = isNationalType(type);

    byte[] bytes;
    if (isNational) {
      bytes = stringVal.getBytes(StandardCharsets.UTF_16LE);
    } else {
      bytes = stringVal.getBytes(context.varcharCharset());
    }

    if (bytes.length > 8000) {
      writePlp(buf, bytes);
    } else {
      buf.putShort((short) bytes.length);
      buf.put(bytes);
    }
  }

  private boolean isNationalType(TdsType type) {
    return type == TdsType.NVARCHAR || type == TdsType.NCHAR || type == TdsType.NTEXT;
  }

  private boolean isLargeString(ParamEntry entry) {
    Object val = entry.value().getValue();
    if (val instanceof String s) {
      // Threshold depends on whether it's 2-byte UTF-16 or 1-byte varchar
      return isNationalType(entry.key().type()) ? s.length() > 4000 : s.length() > 8000;
    }
    return false;
  }

  private int getEncodedLength(ParamEntry entry, RpcEncodingContext context) {
    Object value = entry.value().getValue();
    if (value == null) {
      return 0;
    }

    if (value instanceof String s) {
      if (isNationalType(entry.key().type())) {
        return s.length() * 2;
      }
      return s.getBytes(context.varcharCharset()).length;
    }
    return 0;
  }

  private void writeNull(ByteBuffer buf, TdsType type) {
    if (type.strategy == TdsType.LengthStrategy.PLP) {
      buf.putLong(0xFFFFFFFFFFFFFFFFL);
    } else {
      buf.putShort((short) 0xFFFF);
    }
  }

  private void writePlp(ByteBuffer buf, byte[] data) {
    buf.putLong(data.length); // Total PLP length
    buf.putInt(data.length);  // Chunk length
    buf.put(data);            // Chunk data
    buf.putInt(0);            // PLP Terminator
  }
}