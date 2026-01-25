package org.tdslib.javatdslib.query.rpc;

import java.math.BigDecimal;
import java.sql.*;

/**
 * Immutable entry for one RPC parameter.
 * Contains everything needed to write TYPE_INFO + value in TDS RPC stream.
 */
public record ParamEntry(
        BindingKey key,               // The original binding key
        Object value
) {
  public enum TypeLengthType {
    FIXED,      // no length prefix needed
    VARLEN,     // USHORT max length prefix
    PLP         // 0xFFFFFFFF + chunked data (for max types)
  }// Convenience constructor from BindingKey + value

}