package org.tdslib.javatdslib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.done.DoneToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.error.ErrorToken;
import org.tdslib.javatdslib.transport.ConnectionContext;
import org.tdslib.javatdslib.transport.EnvChangeApplier;
import org.tdslib.javatdslib.transport.TdsTransport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LoginResponse implements TokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(LoginResponse.class);

  private boolean success = true; // Assume true until an error token breaks it
  private String errorMessage = null;
  private String database = null;

  private final List<EnvChangeToken> envChanges = new ArrayList<>();
  private final TdsTransport transport;
  private final ConnectionContext context;

  public LoginResponse(TdsTransport transport, ConnectionContext context) {
    this.transport = transport;
    this.context = context;
  }

  @Override
  public void onToken(Token token, QueryContext queryContext) {
    switch (token.getType()) {
      case ENV_CHANGE:
        EnvChangeToken envToken = (EnvChangeToken) token;
        EnvChangeApplier.apply(envToken, context);
        envChanges.add(envToken);
        break;
      case ERROR:
        ErrorToken errToken = (ErrorToken) token;
        setErrorMessage(errToken.getMessage());
        break;
      case DONE:
      case DONE_PROC:
      case DONE_IN_PROC:
        DoneToken done = (DoneToken) token;
        // Fix: Use hasError() instead of isError()
        if (done.getStatus().hasError()) {
          this.success = false;
        }
        break;
      case LOGIN_ACK:
        this.success = true;
        break;
      default:
        break;
    }
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
    this.success = false; // error implies failure
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void addEnvChange(EnvChangeToken change) {
    envChanges.add(change);
  }

  public boolean isSuccess() {
    return success;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getDatabase() {
    return database;
  }

  public List<EnvChangeToken> getEnvChanges() {
    return Collections.unmodifiableList(envChanges);
  }

  public boolean hasEnvChanges() {
    return !envChanges.isEmpty();
  }
}