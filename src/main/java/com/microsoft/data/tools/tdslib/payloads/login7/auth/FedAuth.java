// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.payloads.login7.auth;

import java.nio.ByteBuffer;

/** Federated authentication feature extension base. */
public abstract class FedAuth {
    protected static final byte FeatureId = 0x02;
    protected static final byte LibrarySecurityToken = 0x02;
    protected static final byte LibraryADAL = 0x04;
    protected static final byte FedAuthEchoYes = 0x01;
    protected static final byte FedAuthEchoNo = 0x00;

    protected FedAuth() { }

    public abstract ByteBuffer getBuffer();
}
