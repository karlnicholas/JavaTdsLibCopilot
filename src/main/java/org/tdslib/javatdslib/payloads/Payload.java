// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.payloads;

import java.nio.ByteBuffer;

/**
 * Payload for messages.
 */
public abstract class Payload {
    protected ByteBuffer buffer;

    /**
     * The buffer with the payload data.
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }

    /**
     * Internally builds the payload buffer.
     */
    protected abstract void buildBufferInternal();

    /**
     * Builds the payload buffer.
     */
    public ByteBuffer buildBuffer() {
        buildBufferInternal();
        return buffer;
    }
}