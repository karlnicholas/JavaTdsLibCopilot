package org.tdslib.javatdslib.tokens;

/**
 * Visitor/callback interface that receives each successfully parsed token.
 */
@FunctionalInterface
public interface TokenVisitor {

  /**
   * Called when a token is successfully parsed.
   *
   * @param token The parsed token.
   */
  void onToken(Token token);

  // NEW: Dedicated out-of-band error channel

  /**
   * Called when an error occurs during parsing.
   *
   * @param t The error that occurred.
   */
  default void onError(Throwable t) {
  }
}
