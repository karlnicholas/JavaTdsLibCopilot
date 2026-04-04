// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.packets;

/**
 * Type of Packet.
 */
public enum PacketType {
  /**
   * Unknown packet type.
   */
  UNKNOWN((byte) 0x00, "Unknown"),
  /**
   * SQL batch.
   */
  SQL_BATCH((byte) 0x01, "SQL Batch"),
  /**
   * RPC.
   */
  RPC_REQUEST((byte) 0x03, "RPC"),
  /**
   * Tabular result.
   */
  TABULAR_RESULT((byte) 0x04, "Tabular Result"),
  /**
   * Attention signal.
   */
  ATTENTION((byte) 0x06, "Attention"),
  /**
   * Bulk load data.
   */
  BULK_LOAD((byte) 0x07, "Bulk Load"),
  /**
   * Federated Authentication Token.
   */
  FED_AUTH_TOKEN((byte) 0x08, "Federated Auth Token"),
  /**
   * Transaction manager request.
   */
  TRANSACTION_MANAGER((byte) 0x0E, "Transaction Manager"),
  /**
   * TDS7 Login.
   */
  LOGIN7((byte) 0x10, "Login7"),
  /**
   * SSPI (Security Support Provider Interface).
   */
  SSPI((byte) 0x11, "SSPI"),
  /**
   * Pre-Login.
   */
  PRE_LOGIN((byte) 0x12, "PreLogin");

  private final byte value;
  private final String name;

  // Static lookup table for performance
  private static final PacketType[] LOOKUP = new PacketType[256];

  static {
    for (PacketType type : PacketType.values()) {
      LOOKUP[type.value & 0xFF] = type;
    }
  }

  PacketType(byte value, String name) {
    this.value = value;
    this.name = name;
  }

  public byte getValue() {
    return value;
  }

  public String getName() {
    return name;
  }

  /**
   * Translates a byte value into a PacketType.
   *
   * @param value The raw byte from the TDS header.
   * @return The corresponding PacketType or UNKNOWN if not found.
   */
  public static PacketType valueOf(int value) {
    PacketType type = LOOKUP[value & 0xFF];
    return (type != null) ? type : UNKNOWN;
  }
}