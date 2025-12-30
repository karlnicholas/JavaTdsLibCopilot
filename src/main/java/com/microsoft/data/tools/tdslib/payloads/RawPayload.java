// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.payloads;

import java.nio.ByteBuffer;

/**
 * Raw payload.
 */
public class RawPayload extends Payload {
    public RawPayload(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    protected void buildBufferInternal() {
        // Already built
    }
}