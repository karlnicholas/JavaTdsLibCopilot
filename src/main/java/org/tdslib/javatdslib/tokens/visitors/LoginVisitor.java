package org.tdslib.javatdslib.tokens.visitors;

import org.tdslib.javatdslib.QueryContext;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.done.DoneToken;
import org.tdslib.javatdslib.tokens.error.ErrorToken;

public class LoginVisitor implements TokenVisitor {
  private boolean success = true;
  private String errorMessage = null;

  @Override
  public void onToken(Token token, QueryContext queryContext) {
    switch (token.getType()) {
      case ERROR:
        this.success = false;
        this.errorMessage = ((ErrorToken) token).getMessage();
        break;
      case DONE:
      case DONE_PROC:
      case DONE_IN_PROC:
        if (((DoneToken) token).getStatus().hasError()) {
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

  public boolean isSuccess() { return success; }
  public String getErrorMessage() { return errorMessage; }
}