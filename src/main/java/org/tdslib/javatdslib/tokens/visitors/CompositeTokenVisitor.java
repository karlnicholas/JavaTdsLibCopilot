  package org.tdslib.javatdslib.tokens.visitors;

import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenVisitor;

import java.util.List;

public class CompositeTokenVisitor implements TokenVisitor {
  private final List<TokenVisitor> visitors;

  public CompositeTokenVisitor(TokenVisitor... visitors) {
    this.visitors = List.of(visitors);
  }

  @Override
  public void onToken(Token token, QueryContext queryContext) {
    for (TokenVisitor visitor : visitors) {
      visitor.onToken(token, queryContext);
    }
  }
}