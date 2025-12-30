// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens.done;

/**
 * Statement in a procedure done.
 */
public final class DoneProcToken extends DoneToken {
    public DoneProcToken(DoneStatus status, int currentCommand, long rowCount) {
        super(status, currentCommand, rowCount);
    }

    @Override
    public com.microsoft.data.tools.tdslib.tokens.TokenType getType() {
        return com.microsoft.data.tools.tdslib.tokens.TokenType.DONE_PROC;
    }
}
