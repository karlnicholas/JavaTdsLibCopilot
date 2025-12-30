// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


package com.microsoft.data.tools.tdslib.tokens.done;

import com.microsoft.data.tools.tdslib.tokens.TokenType;

/**
 * Statement in a procedure done.
 */
public final class DoneProcToken extends DoneToken {
    public DoneProcToken(DoneStatus status, int currentCommand, long rowCount) {
        super(status, currentCommand, rowCount);
    }

    @Override
    public TokenType getType() {
        return TokenType.DONE_PROC;
    }
}
