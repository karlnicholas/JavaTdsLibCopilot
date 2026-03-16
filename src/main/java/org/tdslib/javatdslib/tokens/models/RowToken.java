package org.tdslib.javatdslib.tokens.models;

import org.tdslib.javatdslib.tokens.Token;
import org.tdslib.javatdslib.tokens.TokenType;

/**
 * A lightweight token representing the start of a ROW (0xD1).
 * It holds a reference to the active ColMetaDataToken that describes this row's structure.
 */
public class RowToken extends Token {
  private final ColMetaDataToken metaData;

  public RowToken(ColMetaDataToken metaData) {
    super(TokenType.ROW);
    this.metaData = metaData;
  }

  public ColMetaDataToken getMetaData() {
    return metaData;
  }
}