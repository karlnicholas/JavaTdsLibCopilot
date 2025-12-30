// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


package org.tdslib.javatdslib.tokens.done;

import org.tdslib.javatdslib.tokens.TokenType;

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
