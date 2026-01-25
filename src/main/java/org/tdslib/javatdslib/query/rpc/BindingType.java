package org.tdslib.javatdslib.query.rpc;

enum BindingType {
  SHORT((byte) 0x26),
  INTEGER((byte) 0x26),
  LONG((byte) 0x26),
  BYTE((byte) 0x26),
  BOOLEAN((byte) 0x68),
  FLOAT((byte) 0x6D),
  DOUBLE((byte) 0x6D),
  BIGDECIMAL((byte) 0x6A),
  STRING((byte) 0xE7),
  BYTES((byte) 0xA5),
  DATE((byte) 0x28),
  TIME((byte) 0x29),
  TIMESTAMP((byte) 0x2A),
  CLOB((byte) 0xE7),
  NCLOB((byte) 0xE7),
  BLOB((byte) 0xA5),
  SQLXML((byte) 0xF1);
  // Fields
  private final byte xtype;

  BindingType(byte xtype) {
    this.xtype = xtype;
  }

  public byte getXtype() {
    return this.xtype;
  }
}
