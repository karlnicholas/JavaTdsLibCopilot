package org.tdslib.javatdslib.tokens;

/**
 * Interface representing a sink for decoded TDS tokens and column data.
 */
public interface TdsDecoderSink {
  /**
   * Called when a complete token is decoded.
   *
   * @param token The decoded token.
   */
  void onToken(Token token);

  /**
   * Called when column data is decoded.
   *
   * @param data The column data.
   */
  void onColumnData(ColumnData data);

  /**
   * Called when an error occurs during decoding.
   *
   * @param error The error.
   */
  void onError(Throwable error);
}