// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.tokens;

/**
 * Token event.
 */
public class TokenEvent {
    private Token token;
    private boolean exit;

    /**
     * The token that was received.
     */
    public Token getToken() {
        return token;
    }

    public void setToken(Token token) {
        this.token = token;
    }

    /**
     * Indicate if the token handler should stop receiving tokens.
     */
    public boolean isExit() {
        return exit;
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }
}