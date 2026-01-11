package org.tdslib.javatdslib;

import org.tdslib.javatdslib.tokens.colmetadata.ColMetaDataToken;

/**
 * Holds per-query parsing context such as the last seen column metadata token.
 */
public class QueryContext {
  private ColMetaDataToken colMetaDataToken;

  public void setColMetaDataToken(ColMetaDataToken colMetaDataToken) {
    this.colMetaDataToken = colMetaDataToken;
  }

  public ColMetaDataToken getColMetaDataToken() {
    return colMetaDataToken;
  }

}
