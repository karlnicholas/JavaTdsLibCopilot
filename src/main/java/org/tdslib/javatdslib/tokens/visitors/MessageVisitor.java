package org.tdslib.javatdslib.tokens.visitors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.protocol.TdsServerErrorException;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.models.ErrorToken;
import org.tdslib.javatdslib.tokens.models.InfoToken;

import java.util.function.Consumer;

public class MessageVisitor implements TokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(MessageVisitor.class);
  private static final String SERVER_MESSAGE = "Server message [{}] (state {}): {}";

  private final Consumer<Throwable> errorHandler;

  public MessageVisitor(Consumer<Throwable> errorHandler) {
    this.errorHandler = errorHandler;
  }

  @Override
  public void onToken(Token token) {
    if (token.getType() == TokenType.INFO) {
      InfoToken info = (InfoToken) token;
      logger.info(SERVER_MESSAGE, info.getNumber(), info.getState(), info.getMessage());

    } else if (token.getType() == TokenType.ERROR) {
      ErrorToken err = (ErrorToken) token;
      logger.error(SERVER_MESSAGE, err.getNumber(), err.getState(), err.getMessage());

      if (err.isError() && errorHandler != null) {
// Create the generic, API-agnostic exception
        TdsServerErrorException tdsError = new TdsServerErrorException(
            err.getMessage(),
            (int) err.getNumber(),
            err.getState(),
            err.getSeverity(), // FIX: Was err.getClassLevel()
            err.getServerName(),
            err.getProcName(),
            (int) err.getLineNumber()
        );

        errorHandler.accept(tdsError);
      }
    }
  }
}