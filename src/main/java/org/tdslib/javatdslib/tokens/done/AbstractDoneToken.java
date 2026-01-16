package org.tdslib.javatdslib.tokens.done;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * Abstract base class for all DONE family tokens (DONE, DONE_IN_PROC, DONE_PROC).
 * Contains shared fields and logic.
 */
public abstract class AbstractDoneToken extends Token {

  private final DoneStatus status;
  private final int currentCommand;
  private final long rowCount;

  /**
   * Construct an AbstractDoneToken.
   *
   * @param type           raw token byte value
   * @param status         DONE status flags (may be null)
   * @param currentCommand command id associated with the token
   * @param rowCount       row count reported by the token
   */
  protected AbstractDoneToken(final byte type, final DoneStatus status,
                              final int currentCommand, final long rowCount) {
    super(TokenType.fromValue(type));
    this.status = status;
    this.currentCommand = currentCommand;
    this.rowCount = rowCount;
  }

  /**
   * Status flags for this DONE token.
   *
   * @return the DoneStatus for this token
   */
  public DoneStatus getStatus() {
    return status;
  }

  /**
   * The current command number (1-based) this DONE refers to.
   *
   * @return current command id
   */
  public int getCurrentCommand() {
    return currentCommand;
  }

  /**
   * Row count associated with this DONE token (if present).
   *
   * @return row count
   */
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{"
        + "status=" + status
        + ", cmd=" + currentCommand
        + ", rows=" + rowCount
        + '}';
  }
}