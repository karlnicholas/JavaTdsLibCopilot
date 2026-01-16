package org.tdslib.javatdslib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenVisitor;
import org.tdslib.javatdslib.tokens.done.DoneToken;
import org.tdslib.javatdslib.tokens.envchange.EnvChangeToken;
import org.tdslib.javatdslib.tokens.error.ErrorToken;
import org.tdslib.javatdslib.tokens.info.InfoToken;
import org.tdslib.javatdslib.tokens.loginack.LoginAckToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the result of a Login7 request.
 * Collects success/failure status and any side-effects (environment changes, errors).
 */
public class LoginResponse implements TokenVisitor {
  private static final Logger logger = LoggerFactory.getLogger(LoginResponse.class);

  private boolean success = false;
  private String errorMessage = null;
  private String database = null;
  private final ConnectionContext connectionContext;
  private final EnvChangeTokenVisitor envChangeTokenVisitor;

  private final List<EnvChangeToken> envChanges = new ArrayList<>();

  // --- Mutators (used during token processing) ---

  /**
   * Create a LoginResponse that will apply tokens to the provided ConnectionContext.
   *
   * @param connectionContext target connection context to update
   */
  public LoginResponse(ConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.envChangeTokenVisitor = new EnvChangeTokenVisitor(connectionContext);
  }

  @Override
  public void onToken(Token token) {
    if (token instanceof EnvChangeToken envChange) {
      envChangeTokenVisitor.applyEnvChange(envChange);
    } else if (token instanceof LoginAckToken ack) {
      connectionContext.setTdsVersion(ack.getTdsVersion());
      connectionContext.setServerName(ack.getServerName());
      connectionContext.setServerVersionString(ack.getServerVersionString());
      setSuccess(true);
      logger.info(
          "Login successful - TDS version: {}, Server name: {}, Server version: {}",
          ack.getTdsVersion(),
          ack.getServerName(),
          ack.getServerVersionString()
      );
    } else if (token instanceof ErrorToken err) {
      logger.warn(
          "Server error [{}]: {}",
          err.getNumber(),
          err.getMessage()
      );
    } else if (token instanceof InfoToken info) {
      // Severity 0–10 = info, >10 = error (but INFO token is always <=10)
      logger.info(
          "Server info [{}] (state {}): {}",
          info.getNumber(),
          info.getState(),
          info.getMessage()
      );
    } else if (token instanceof DoneToken done) {
      logger.debug("Batch completed (status: {})", done.getStatus());
    } else {
      logger.debug("Unhandled token type: {}", token.getType());
    }
  }

  /**
   * Mark the login result as successful or not.
   *
   * @param success true if login succeeded
   */
  public void setSuccess(boolean success) {
    this.success = success;
  }

  /**
   * Set an error message for the login result and mark as failed.
   *
   * @param errorMessage textual error description
   */
  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
    this.success = false; // error implies failure
  }

  /**
   * Record the negotiated/default database name.
   *
   * @param database database name
   */
  public void setDatabase(String database) {
    this.database = database;
  }

  /**
   * Add an ENVCHANGE token to the recorded list (used for diagnostics).
   *
   * @param change the envchange token to record
   */
  public void addEnvChange(EnvChangeToken change) {
    if (change != null) {
      envChanges.add(change);
    }
  }

  // --- Accessors ---

  public boolean isSuccess() {
    return success;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getDatabase() {
    return database;
  }

  /**
   * Returns an unmodifiable view of the collected environment changes.
   */
  public List<EnvChangeToken> getEnvChanges() {
    return Collections.unmodifiableList(envChanges);
  }

  /**
   * Convenience: returns true if any ENVCHANGE tokens were received.
   */
  public boolean hasEnvChanges() {
    return !envChanges.isEmpty();
  }

  @Override
  public String toString() {
    return "LoginResponse{"
        + "success=" + success
        + ", errorMessage='" + errorMessage + '\''
        + ", database='" + database + '\''
        + ", envChanges=" + envChanges.size() + " item(s)"
        + '}';
  }
}
