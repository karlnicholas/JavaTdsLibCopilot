package org.tdslib.javatdslib.query.rpc;

import java.math.BigDecimal;
import java.sql.*;

/**
 * Immutable entry for one RPC parameter.
 * Contains everything needed to write TYPE_INFO + value in TDS RPC stream.
 */
public record ParamEntry(
        BindingKey key,               // The original binding key
        Object value,                 // the actual value (or null)
        byte xtype,                   // primary TDS type code (e.g., 0xF1 for XMLTYPE)
        byte nullableXtype,           // nullable variant if different (often same)
        TypeLengthType lengthType,    // FIXED / VARLEN / PLP
        int maxLengthBytes,           // for varlen: max bytes (or -1 for PLP/max)
        boolean needsCollation,       // true → write 5-byte TDSCOLLATION after type
        String collation,             // e.g. "Latin1_General_100_CI_AS" or null
        byte[] extraTypeInfo,         // optional extra bytes (scale, precision+scale, schema flag, etc.)
        BindingType bindingType       // original enum for debugging / dispatch
) {
  public enum TypeLengthType {
    FIXED,      // no length prefix needed
    VARLEN,     // USHORT max length prefix
    PLP         // 0xFFFFFFFF + chunked data (for max types)
  }// Convenience constructor from BindingKey + value

  public ParamEntry(BindingKey key, Object value) {
    this(
            key,
            value,
            key.type().getXtype(false),
            key.type().getXtype(true),
            switch (key.type().getLengthType()) {
              case "fixed" -> TypeLengthType.FIXED;
              case "varlen" -> TypeLengthType.VARLEN;
              default -> TypeLengthType.PLP;
            },
            key.type().getDefaultMaxLength(),
            key.type().isNeedsCollation(),
            key.type().isNeedsCollation() ? "Latin1_General_100_CI_AS_KS_WS_SC_UTF8" : null,  // default collation
            computeExtraInfo(key.type(), value),
            key.type()
    );
  }

  private static byte[] computeExtraInfo(BindingType bt, Object value) {
    return switch (bt) {
      case TIME, TIMESTAMP -> new byte[]{0x07}; // scale 7 (common default)
      case BIGDECIMAL -> {
        if (value instanceof BigDecimal bd) {
          yield new byte[]{(byte) bd.precision(), (byte) bd.scale()};
        }
        yield new byte[]{18, 10}; // default
      }
      case SQLXML -> new byte[]{0x00}; // schema present = false (common)
      case STRING, CLOB, NCLOB -> new byte[0]; // collation handled separately
      case BYTES, BLOB -> new byte[0];
      default -> new byte[0];
    };
  }// Helper: is null?

  public boolean isNull() {
    return value == null;
  }// Helper: effective xtype

  public byte getEffectiveXtype() {
    return isNull() ? nullableXtype : xtype;
  }// Helper: should use PLP?

  public boolean usePlp() {
    return lengthType == TypeLengthType.PLP || (maxLengthBytes < 0 && valueRequiresPlp(value));
  }

  private static boolean valueRequiresPlp(Object value) {
    if (value instanceof String str) {
      return str.length() > 4000;
    } else if (value instanceof byte[] bytes) {
      return bytes.length > 8000;
    } // add for Clob/Blob if needed
    return false;
  }
}