package org.tdslib.javatdslib;

import org.tdslib.javatdslib.tokens.metadata.ColMetaDataToken;

public class QueryContext {
    private ColMetaDataToken colMetaDataToken;

    public void setColMetaDataToken(ColMetaDataToken colMetaDataToken) {
        this.colMetaDataToken = colMetaDataToken;
    }

    public ColMetaDataToken getColMetaDataToken() {
        return colMetaDataToken;
    }

}
