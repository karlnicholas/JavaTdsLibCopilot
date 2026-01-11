// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.payloads;

import java.nio.ByteBuffer;

/**
 * Raw payload.
 */
public class RawPayload extends Payload {
  /**
   * Create a raw payload backed by the provided buffer.
   *
   * @param buffer backing buffer (already prepared)
   */
  public RawPayload(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void buildBufferInternal() {
    // Already built
  }
}