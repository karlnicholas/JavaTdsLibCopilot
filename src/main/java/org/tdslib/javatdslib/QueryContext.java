package org.tdslib.javatdslib;

import org.tdslib.javatdslib.tokens.colmetadata.ColMetaDataToken;
import org.tdslib.javatdslib.tokens.returnvalue.ReturnValueToken;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds per-query parsing context such as the last seen column metadata token.
 */
public class QueryContext {
  private ColMetaDataToken colMetaDataToken;
  // ------------------- State -------------------
  private final List<ReturnValueToken> returnValues;
  private boolean hasError;

  public QueryContext() {
    returnValues = new ArrayList<>();
    hasError = false;
  }

  public List<ReturnValueToken> getReturnValues() {
    return returnValues;
  }

  public void addReturnValue(ReturnValueToken returnValueToken) {
    returnValues.add(returnValueToken);
  }

  public boolean isHasError() {
    return hasError;
  }

  public void setHasError(boolean hasError) {
    this.hasError = hasError;
  }

  public void setColMetaDataToken(ColMetaDataToken colMetaDataToken) {
    this.colMetaDataToken = colMetaDataToken;
  }

  public ColMetaDataToken getColMetaDataToken() {
    return colMetaDataToken;
  }

}
