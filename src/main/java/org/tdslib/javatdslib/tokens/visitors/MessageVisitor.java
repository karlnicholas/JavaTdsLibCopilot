package org.tdslib.javatdslib.tokens.visitors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.SqlErrorTranslator;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.error.ErrorToken;
import org.tdslib.javatdslib.tokens.info.InfoToken;

import java.util.function.Consumer;

public class MessageVisitor implements TokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(MessageVisitor.class);
  private static final String SERVER_MESSAGE = "Server message [{}] (state {}): {}";

  private final Consumer<Throwable> errorHandler;

  public MessageVisitor(Consumer<Throwable> errorHandler) {
    this.errorHandler = errorHandler;
  }

  @Override
  public void onToken(Token token, QueryContext queryContext) {
    if (token.getType() == TokenType.INFO) {
      InfoToken info = (InfoToken) token;
      logger.info(SERVER_MESSAGE, info.getNumber(), info.getState(), info.getMessage());
      if (info.isError()) queryContext.setHasError(true);

    } else if (token.getType() == TokenType.ERROR) {
      ErrorToken err = (ErrorToken) token;
      logger.error(SERVER_MESSAGE, err.getNumber(), err.getState(), err.getMessage());

      if (err.isError()) {
        queryContext.setHasError(true);
        if (errorHandler != null) {
          errorHandler.accept(SqlErrorTranslator.createException(err));
        }
      }
    }
  }
}