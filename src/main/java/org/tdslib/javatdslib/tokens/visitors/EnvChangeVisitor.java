package org.tdslib.javatdslib.tokens.visitors;

import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.EnvChangeApplier;

public class EnvChangeVisitor implements TokenVisitor {
  private final ConnectionContext context;

  public EnvChangeVisitor(ConnectionContext context) {
    this.context = context;
  }

  @Override
  public void onToken(Token token, QueryContext queryContext) {
    if (token.getType() == TokenType.ENV_CHANGE) {
      EnvChangeApplier.apply((EnvChangeToken) token, context);
    }
  }
}