// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.packets;

/**
 * Packet header offsets.
 */
class PacketOffset {
    static final int TYPE = 0;
    static final int STATUS = 1;
    static final int LENGTH = 2;
    static final int SPID = 4;
    static final int PACKET_ID = 6;
    static final int WINDOW = 7;
}