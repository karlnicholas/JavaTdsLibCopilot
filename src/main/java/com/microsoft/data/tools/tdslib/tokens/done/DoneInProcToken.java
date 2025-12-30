// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


package com.microsoft.data.tools.tdslib.tokens.done;

import com.microsoft.data.tools.tdslib.tokens.TokenType;

/**
 * Token indicating the completion status of a statement in a procedure.
 */
public final class DoneInProcToken extends DoneToken {
    public DoneInProcToken(DoneStatus status, int currentCommand, long rowCount) {
        super(status, currentCommand, rowCount);
    }

    @Override
    public TokenType getType() {
        return TokenType.DONE_IN_PROC;
    }
}
