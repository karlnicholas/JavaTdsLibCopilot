package org.tdslib.javatdslib.tokens.visitors;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenVisitor;

import java.util.List;

/**
 * A {@link TokenVisitor} that delegates token processing to a list of other visitors. This allows
 * multiple visitors to process the same stream of tokens sequentially.
 */
public class CompositeTokenVisitor implements TokenVisitor {
  private final List<TokenVisitor> visitors;

  /**
   * Constructs a new CompositeTokenVisitor.
   *
   * @param visitors The list of visitors to delegate to.
   */
  public CompositeTokenVisitor(TokenVisitor... visitors) {
    this.visitors = List.of(visitors);
  }

  @Override
  public void onToken(Token token) {
    for (TokenVisitor visitor : visitors) {
      visitor.onToken(token);
    }
  }
}
