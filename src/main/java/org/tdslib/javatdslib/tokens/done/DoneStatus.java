package org.tdslib.javatdslib.tokens.done;

/**
 * Wraps the raw status bits of a DONE token and provides safe accessors.
 */
public class DoneStatus {

  private final int value;

  // Constants for internal bitwise logic
  private static final int BIT_MORE = 0x0001;
  private static final int BIT_ERROR = 0x0002;
  private static final int BIT_IN_XACT = 0x0004;
  private static final int BIT_COUNT = 0x0010;
  private static final int BIT_ATTN = 0x0020;

  /**
   * Create a DoneStatus wrapping the raw status bits from a DONE token.
   *
   * @param value raw status bits as an integer
   */
  public DoneStatus(int value) {
    this.value = value;
  }

  /**
   * Indicates whether the DONE token signals more result sets follow.
   *
   * @return true if the MORE bit is set
   */
  public boolean hasMoreResults() {
    return (value & BIT_MORE) != 0;
  }

  /**
   * Indicates whether the DONE token signals an error occurred.
   *
   * @return true if the ERROR bit is set
   */
  public boolean hasError() {
    return (value & BIT_ERROR) != 0;
  }

  public boolean isValidRowCount() {
    return (value & BIT_COUNT) != 0;
  }

  public boolean isTransactionInProgress() {
    return (value & BIT_IN_XACT) != 0;
  }

  public boolean isFinal() {
    // Technically "Final" just means the MORE bit is NOT set
    return !hasMoreResults();
  }

  @Override
  public String toString() {
    return "DoneStatus(raw=" + Integer.toHexString(value) + ")";
  }
}
