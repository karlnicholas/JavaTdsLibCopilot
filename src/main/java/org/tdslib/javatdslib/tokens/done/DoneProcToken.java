package org.tdslib.javatdslib.tokens.done;

/**
 * DONE_PROC token (0xFE) - completion of a stored procedure or RPC.
 */
public final class DoneProcToken extends DoneToken {

  /**
   * Construct a DONE_PROC token.
   *
   * @param type           raw token byte value
   * @param status         DONE status flags
   * @param currentCommand command id associated with the token
   * @param rowCount       row count reported by the token
   */
  public DoneProcToken(final byte type,
                       final DoneStatus status,
                       final int currentCommand,
                       final long rowCount) {
    super(type, status, currentCommand, rowCount);
  }

}