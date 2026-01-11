package org.tdslib.javatdslib.tokens.done;

/**
 * Standard DONE token (0xFD) - marks end of a batch or statement.
 */
public final class DoneToken extends AbstractDoneToken {

  /**
   * Construct a standard DONE token.
   *
   * @param type           raw token byte value
   * @param status         DONE status flags
   * @param currentCommand command id associated with the token
   * @param rowCount       row count reported by the token
   */
  public DoneToken(final byte type,
                   final DoneStatus status,
                   final int currentCommand,
                   final long rowCount) {
    super(type, status, currentCommand, rowCount);
  }

}