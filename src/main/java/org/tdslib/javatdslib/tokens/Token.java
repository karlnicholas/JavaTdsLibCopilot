// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens;

/**
 * Represents a TDS data stream token.
 *
 * <p>Each token has a {@link TokenType} indicating the kind of payload
 * carried by the token (for example: INFO, ERROR, ROW, DONE, etc.).
 */
public abstract class Token {
  private final TokenType type;

  /**
   * Create a token with the specified type.
   *
   * @param type the token type.
   */
  protected Token(final TokenType type) {
    this.type = type;
  }

  /**
   * Returns the type of the token.
   *
   * @return the token type.
   */
  public final TokenType getType() {
    return type;
  }

}
