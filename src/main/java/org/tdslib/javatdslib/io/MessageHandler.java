// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.io;

import org.tdslib.javatdslib.messages.Message;
import org.tdslib.javatdslib.messages.TdsPacketReader;
import org.tdslib.javatdslib.messages.TdsPacketWriter;
import org.tdslib.javatdslib.transport.TcpTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles sending and receiving complete TDS messages.
 * <p>
 * A TDS message may span multiple packets.
 * This class provides both low-level (single packet) and higher-level
 * (full logical message) send/receive operations.
 */
public final class MessageHandler {

    private final TcpTransport transport;
    private final TdsPacketReader packetReader;
    private final TdsPacketWriter packetWriter;

    private short currentPacketNumber = 1;  // Increments per packet sent

    public MessageHandler(TcpTransport transport) {
        this.transport = transport;
        this.packetReader = new TdsPacketReader(transport);
        this.packetWriter = new TdsPacketWriter();
    }

    /**
     * Sends a complete logical message (may be split into multiple packets).
     *
     * @param message the message to send (usually built by the client layer)
     * @throws IOException if sending fails
     */
    public void sendMessage(Message message) throws IOException {
        // If the message payload is small, send as single packet
        // If large, split into multiple packets (max ~4096 bytes each)
        List<ByteBuffer> packetBuffers = packetWriter.buildPackets(
                message.getPacketType(),
                message.getStatusFlags(),
                message.getPayload(),
                currentPacketNumber,
                transport.getCurrentPacketSize()
        );

        for (ByteBuffer buf : packetBuffers) {
            transport.write(buf);
            currentPacketNumber++;
        }

        transport.flush();
    }

    /**
     * Receives **one single TDS packet** and wraps it as a Message.
     * <p>
     * This is the most basic receive operation.
     * For full logical responses, the caller should loop until isLastPacket().
     *
     * @return one complete TDS packet as Message
     * @throws IOException if reading fails
     */
    public Message receiveSinglePacket() throws IOException {
        // The packet reader handles header + payload reading
        ByteBuffer rawPacket = packetReader.readRawPacket();

        // Parse header (first 8 bytes)
        rawPacket.mark();
        byte type = rawPacket.get();
        byte status = rawPacket.get();
        int length = Short.toUnsignedInt(rawPacket.getShort());
        short spid = rawPacket.getShort();
        short packetId = rawPacket.getShort();
        rawPacket.get(); // window (usually 0)

        // Reset and slice payload
        rawPacket.reset();
        rawPacket.position(8);
        ByteBuffer payload = rawPacket.slice().limit(length - 8);

        return new Message(
                type,
                status,
                length,
                spid,
                packetId,
                payload,
                System.nanoTime(),
                null  // trace context - can be injected later
        );
    }

    /**
     * Receives a **complete logical response** by reading packets until the last one (EOM).
     * <p>
     * Useful for simple request-response patterns.
     *
     * @return list of all packets that form the logical response
     * @throws IOException if any read fails
     */
    public List<Message> receiveFullResponse() throws IOException {
        List<Message> messages = new ArrayList<>();

        Message packet;
        do {
            packet = receiveSinglePacket();
            messages.add(packet);

            // Optional: handle reset connection flag as soon as we see it
            if (packet.isResetConnection()) {
                // Can notify upper layers immediately if needed
            }
        } while (!packet.isLastPacket());

        return messages;
    }

    /**
     * Resets the packet number counter (useful after login or connection reset).
     */
    public void resetPacketNumbering() {
        currentPacketNumber = 1;
    }

    /**
     * Closes the underlying transport (socket).
     */
    public void close() throws IOException {
        transport.close();
    }

    // Optional: getters for testing/observability
    public TdsPacketReader getPacketReader() {
        return packetReader;
    }

    public TdsPacketWriter getPacketWriter() {
        return packetWriter;
    }
}