package org.tdslib.javatdslib.tokens.models;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

import java.util.Collections;
import java.util.List;

/**
 * Order token (0xA9) - informs the client which columns the result set is sorted by.
 */
public final class OrderToken extends Token {

  private final List<Integer> orderedColumns;

  /**
   * Constructs an OrderToken.
   *
   * @param tokenType      raw token type byte
   * @param orderedColumns list of 0-based column indices representing the sort order
   */
  public OrderToken(final byte tokenType, final List<Integer> orderedColumns) {
    super(TokenType.fromValue(tokenType));
    this.orderedColumns = orderedColumns != null ? orderedColumns : Collections.emptyList();
  }

  /**
   * Returns the list of column indices by which the data is ordered.
   */
  public List<Integer> getOrderedColumns() {
    return orderedColumns;
  }

  @Override
  public String toString() {
    return String.format("OrderToken{columns=%s}", orderedColumns);
  }
}