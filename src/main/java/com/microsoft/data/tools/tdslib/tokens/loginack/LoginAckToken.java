// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


package com.microsoft.data.tools.tdslib.tokens.loginack;

import com.microsoft.data.tools.tdslib.tokens.Token;
import com.microsoft.data.tools.tdslib.tokens.TokenType;

import com.microsoft.data.tools.tdslib.TdsVersion;

/**
 * Login7 response.
 */
public class LoginAckToken extends Token {
    private final SqlInterfaceType interfaceType;
    private final TdsVersion tdsVersion;
    private final String progName;
    private final ProgVersion progVersion;

    /**
     * Token type.
     */
    @Override
    public TokenType getType() {
        return TokenType.LOGIN_ACK;
    }

    /**
     * SQL interface type.
     */
    public SqlInterfaceType getInterfaceType() {
        return interfaceType;
    }

    /**
     * Tds version.
     */
    public TdsVersion getTdsVersion() {
        return tdsVersion;
    }

    /**
     * Program name.
     */
    public String getProgName() {
        return progName;
    }

    /**
     * Program version.
     */
    public ProgVersion getProgVersion() {
        return progVersion;
    }

    /**
     * Creates a new instance of this token.
     */
    public LoginAckToken(SqlInterfaceType interfaceType, TdsVersion tdsVersion, String progName, ProgVersion progVersion) {
        this.interfaceType = interfaceType;
        this.tdsVersion = tdsVersion;
        this.progName = progName;
        this.progVersion = progVersion;
    }

    /**
     * Gets a human readable string representation of this token.
     */
    @Override
    public String toString() {
        return "LoginAckToken[InterfaceType=" + interfaceType + ", TdsVersion=" + tdsVersion + ", ProgName=" + progName + ", ProgVersion=" + progVersion + "]";
    }
}