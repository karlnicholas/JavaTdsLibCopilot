package org.tdslib.javatdslib.tokens;

/**
 * Visitor/callback interface that receives each successfully parsed token.
 */
@FunctionalInterface
public interface TokenVisitor {
  void onToken(Token token);

  // NEW: Dedicated out-of-band error channel
  default void onError(Throwable t) {}
}