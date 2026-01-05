// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.messages;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.Objects;

/**
 * Represents a single TDS (Tabular Data Stream) packet/message.
 * <p>
 * This class encapsulates the TDS packet header and payload as received from the server
 * or prepared for sending to the server. It is immutable once created.
 * <p>
 * See MS-TDS specification for packet header format:
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/0b993c2a-1a4c-4c1c-8e6f-4f2d7b0f0d5e
 */
public final class Message {

    // ── Core TDS Packet Header Fields ──────────────────────────────────────

    /**
     * TDS packet type (e.g., 0x01 = SQL Batch, 0x04 = Tabular Result, 0x07 = RPC, 0x03 = Login, etc.)
     */
    private final byte packetType;

    /**
     * Status flags byte (bit field):
     * - 0x01: End of Message (EOM / last packet in logical message)
     * - 0x04: From server (usually set on responses)
     * - 0x08: ResetConnection (reset session state after this packet)
     * - 0x10: Ignored (reserved)
     */
    private final byte statusFlags;

    /**
     * Total length of the packet including the 8-byte header (usually 4096 max)
     */
    private final int packetLength;

    // ── Additional Header Fields ───────────────────────────────────────────

    /**
     * Server Process ID (SPID) - useful for debugging and tracing
     */
    private final short spid;

    /**
     * Packet number within the logical message sequence (starts at 1)
     */
    private final short packetNumber;

    // ── Payload ─────────────────────────────────────────────────────────────

    /**
     * The payload bytes after the 8-byte header, positioned at the beginning.
     * This buffer is read-only and little-endian by default (TDS is LE).
     */
    private final ByteBuffer payload;

    // ── Derived / Convenience Fields ───────────────────────────────────────

    private final boolean isLastPacket;

    // ── Optional Debug / Tracing ───────────────────────────────────────────

    /**
     * Timestamp when this packet was received (nanoseconds since JVM start)
     */
    private final long receivedAt;

    /**
     * Optional tracing context (e.g., OpenTelemetry trace ID, W3C traceparent)
     * Can be null if tracing is not enabled.
     */
    private final String traceContext;

    /**
     * Constructor - typically called by TdsPacketReader after reading the header.
     *
     * @param packetType    TDS packet type byte
     * @param statusFlags   status flags byte
     * @param packetLength  total packet length (including header)
     * @param spid          server process ID
     * @param packetNumber  packet sequence number
     * @param payload       the payload buffer (after header, positioned at 0)
     * @param receivedAt    nano time when packet was received
     * @param traceContext  optional tracing identifier
     */
    public Message(
            byte packetType,
            byte statusFlags,
            int packetLength,
            short spid,
            short packetNumber,
            ByteBuffer payload,
            long receivedAt,
            String traceContext) {

        this.packetType = packetType;
        this.statusFlags = statusFlags;
        this.packetLength = packetLength;
        this.spid = spid;
        this.packetNumber = packetNumber;

        // Ensure payload is read-only and little-endian (TDS is always LE)
        this.payload = Objects.requireNonNull(payload, "Payload cannot be null")
                .asReadOnlyBuffer()
                .order(ByteOrder.LITTLE_ENDIAN);

        this.isLastPacket = (statusFlags & 0x01) != 0;

        this.receivedAt = receivedAt;
        this.traceContext = traceContext; // nullable
    }

    // ── Getters ─────────────────────────────────────────────────────────────

    public byte getPacketType() {
        return packetType;
    }

    public byte getStatusFlags() {
        return statusFlags;
    }

    public int getPacketLength() {
        return packetLength;
    }

    public short getSpid() {
        return spid;
    }

    public short getPacketNumber() {
        return packetNumber;
    }

    public ByteBuffer getPayload() {
        return payload.asReadOnlyBuffer(); // return a new view each time
    }

    public boolean isLastPacket() {
        return isLastPacket;
    }

    public boolean isResetConnection() {
        return (statusFlags & 0x08) != 0;
    }

    public boolean isFromServer() {
        return (statusFlags & 0x04) != 0;
    }

    public long getReceivedAt() {
        return receivedAt;
    }

    public Instant getReceivedInstant() {
        return Instant.ofEpochMilli(System.currentTimeMillis() - (System.nanoTime() - receivedAt) / 1_000_000L);
    }

    public String getTraceContext() {
        return traceContext;
    }

    // ── Utility Methods ─────────────────────────────────────────────────────

    /**
     * Returns a human-readable string representation of the packet type.
     */
    public String getPacketTypeName() {
        return switch (packetType) {
            case 0x01 -> "SQL Batch";
            case 0x03 -> "Login";
            case 0x04 -> "Tabular Result";
            case 0x06 -> "Attention";
            case 0x07 -> "RPC";
            case 0x0F -> "Bulk Load";
            default -> String.format("Unknown (0x%02X)", packetType & 0xFF);
        };
    }

    @Override
    public String toString() {
        return String.format(
                "Message{type=%s (0x%02X), packet=%d, length=%d, spid=%d, last=%b, reset=%b, payloadSize=%d, trace=%s}",
                getPacketTypeName(),
                packetType & 0xFF,
                packetNumber,
                packetLength,
                spid,
                isLastPacket,
                isResetConnection(),
                payload.remaining(),
                traceContext != null ? traceContext : "none"
        );
    }
}