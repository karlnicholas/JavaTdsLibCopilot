package org.tdslib.javatdslib.tokens.visitors;

import org.tdslib.javatdslib.protocol.EnvChangeApplier;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.models.EnvChangeToken;
import org.tdslib.javatdslib.transport.ConnectionContext;

/**
 * A {@link TokenVisitor} that listens for ENVCHANGE tokens and applies the changes to the
 * {@link ConnectionContext}. This ensures that the connection state (e.g., database, charset,
 * packet size) is kept in sync with the server.
 */
public class EnvChangeVisitor implements TokenVisitor {
  private final ConnectionContext context;

  /**
   * Constructs a new EnvChangeVisitor.
   *
   * @param context The connection context to update with environment changes.
   */
  public EnvChangeVisitor(ConnectionContext context) {
    this.context = context;
  }

  @Override
  public void onToken(Token token) {
    if (token.getType() == TokenType.ENV_CHANGE) {
      EnvChangeApplier.apply((EnvChangeToken) token, context);
    }
  }
}
