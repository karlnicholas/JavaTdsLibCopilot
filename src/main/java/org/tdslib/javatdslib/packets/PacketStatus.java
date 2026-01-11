// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.packets;

/**
 * Packet status flags.
 */
public class PacketStatus {
  /**
   * Normal Packet.
   */
  public static final byte NORMAL = 0x00;

  /**
   * End of Message. The last packet in the message.
   */
  public static final byte EOM = 0x01;

  /**
   * Packet/Message to be ignored.
   */
  public static final byte IGNORE = 0x02;

  /**
   * Reset connection.
   */
  public static final byte RESET_CONNECTION = 0x08;

  /**
   * Reset connection but keep transaction state.
   */
  public static final byte RESET_CONNECTION_SKIP_TRAN = 0x10;
}