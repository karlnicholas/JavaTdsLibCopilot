// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.messages;

import com.microsoft.data.tools.tdslib.packets.Packet;
import com.microsoft.data.tools.tdslib.packets.PacketType;
import com.microsoft.data.tools.tdslib.payloads.Payload;

import java.util.List;

/**
 * TDS Message.
 */
public class Message {
    private PacketType packetType;
    private boolean resetConnection;
    private boolean ignore;
    private Payload payload;

    /**
     * The packet type of the packets of this message.
     */
    public PacketType getPacketType() {
        return packetType;
    }

    public void setPacketType(PacketType packetType) {
        this.packetType = packetType;
    }

    /**
     * Indicates if the connection should be reset.
     */
    public boolean isResetConnection() {
        return resetConnection;
    }

    public void setResetConnection(boolean resetConnection) {
        this.resetConnection = resetConnection;
    }

    /**
     * Indicates if the message should be ignored.
     */
    public boolean isIgnore() {
        return ignore;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    /**
     * The payload of the message.
     */
    public Payload getPayload() {
        return payload;
    }

    public void setPayload(Payload payload) {
        this.payload = payload;
    }

    /**
     * Creates a new empty message.
     */
    public Message(PacketType packetType) {
        this(packetType, false);
    }

    public Message(PacketType packetType, boolean resetConnection) {
        this.packetType = packetType;
        this.resetConnection = resetConnection;
    }

    /**
     * Get the packets of this message.
     */
    public List<Packet> getPackets(int packetSize) {
        // TODO: Implement
        return null;
    }

    @Override
    public String toString() {
        return "Message=[Type=" + packetType + ", Reset=" + resetConnection + ", Ignore=" + ignore + ", Payload=" + payload + "]";
    }
}