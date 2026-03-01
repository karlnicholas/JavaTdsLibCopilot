package org.tdslib.javatdslib.protocol.rpc;

/**
 * Enum for TYPE_INFO "styles" in TDS.
 */
enum TypeStyle {
  FIXED,       // Just XTYPE (no extra bytes)
  LENGTH,      // XTYPE + 1-byte length
  PREC_SCALE,  // XTYPE + 1-byte precision + 1-byte scale
  VARLEN       // XTYPE + 2-byte max length
}
