// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tdslib.javatdslib.transport.TcpTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);

    private final TcpTransport transport;
    private final TdsPacketReader packetReader;
    private final TdsPacketWriter packetWriter;

//    private short currentPacketNumber = 1;  // Increments per packet sent

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
        logHex("Sending message", message.getPayload());
        // If large, split into multiple packets (max ~4096 bytes each)
        List<ByteBuffer> packetBuffers = packetWriter.buildPackets(
                message.getPacketType(),
                message.getStatusFlags(),
                message.getPayload(),
                (short) 1,
                transport.getCurrentPacketSize()
        );

        for (ByteBuffer buf : packetBuffers) {
            transport.write(buf);
//            currentPacketNumber++;
        }

        transport.flush();
    }

    // Helper for hex dumping
    private void logHex(String label, ByteBuffer buffer) {
        if (!logger.isDebugEnabled()) return;

        StringBuilder sb = new StringBuilder();
        sb.append(label).append(" (Length: ").append(buffer.remaining()).append(")\n");

        int pos = buffer.position();
        int i = 0;
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            sb.append(String.format("%02X ", b));
            if (++i % 16 == 0) sb.append("\n");
        }
        sb.append("\n");

        // Rewind so the actual write operation can read it again
        buffer.position(pos);
        logger.debug(sb.toString());
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
//    public void resetPacketNumbering() {
//        currentPacketNumber = 1;
//    }

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