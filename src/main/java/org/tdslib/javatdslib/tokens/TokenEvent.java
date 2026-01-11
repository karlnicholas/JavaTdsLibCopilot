// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens;

/**
 * Wrapper representing a token event delivered to handlers.
 */
public class TokenEvent {
  private Token token;
  private boolean exit;

  /**
   * Returns the token that was received.
   *
   * @return the received token.
   */
  public Token getToken() {
    return token;
  }

  /**
   * Set the token for this event.
   *
   * @param token the token to set.
   */
  public void setToken(final Token token) {
    this.token = token;
  }

  /**
   * Indicates whether the token handler should stop receiving tokens.
   *
   * @return true to stop processing further tokens.
   */
  public boolean isExit() {
    return exit;
  }

  /**
   * Mark whether the handler should exit after this event.
   *
   * @param exit true to stop processing.
   */
  public void setExit(final boolean exit) {
    this.exit = exit;
  }
}