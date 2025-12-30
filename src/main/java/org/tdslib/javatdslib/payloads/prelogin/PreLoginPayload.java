// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


package org.tdslib.javatdslib.payloads.prelogin;

import org.tdslib.javatdslib.payloads.Payload;

/**
 * PreLogin payload.
 */
public class PreLoginPayload extends Payload {
    private EncryptionType encryption;
    private SqlVersion version;
    private byte instance;
    private long threadId;
    private byte mars;
    private byte fedAuth;

    /**
     * Encryption.
     */
    public EncryptionType getEncryption() {
        return encryption;
    }

    public void setEncryption(EncryptionType encryption) {
        this.encryption = encryption;
    }

    /**
     * Version.
     */
    public SqlVersion getVersion() {
        return version;
    }

    public void setVersion(SqlVersion version) {
        this.version = version;
    }

    /**
     * Instance Id.
     */
    public byte getInstance() {
        return instance;
    }

    public void setInstance(byte instance) {
        this.instance = instance;
    }

    /**
     * Thread Id.
     */
    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    /**
     * Mars.
     */
    public byte getMars() {
        return mars;
    }

    public void setMars(byte mars) {
        this.mars = mars;
    }

    /**
     * Federate authentication.
     */
    public byte getFedAuth() {
        return fedAuth;
    }

    public void setFedAuth(byte fedAuth) {
        this.fedAuth = fedAuth;
    }

    /**
     * Create a new instance of this class.
     */
    public PreLoginPayload(boolean encrypt) {
        this.encryption = encrypt ? EncryptionType.ON : EncryptionType.OFF;
        this.version = new SqlVersion(11, 0, 0, 0); // Default version
        this.instance = 0;
        this.threadId = Thread.currentThread().getId();
        this.mars = 0;
        this.fedAuth = 0;
        buildBufferInternal();
    }

    /**
     * Create a new instance of this class from a raw buffer.
     */
    public PreLoginPayload(java.nio.ByteBuffer buffer) {
        this.buffer = buffer;
        extractBufferData();
    }

    /**
     * Builds the payload buffer.
     */
    @Override
    protected void buildBufferInternal() {
        // Implements TDS PreLogin packet structure.
        // See: https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/
        // Fields: Version, Encryption, Instance, ThreadId, Mars, FedAuth, Terminator

        // Tag values (from TDS spec)
        final byte TAG_VERSION = 0x00;
        final byte TAG_ENCRYPTION = 0x01;
        final byte TAG_INSTANCE = 0x02;
        final byte TAG_THREADID = 0x03;
        final byte TAG_MARS = 0x04;
        final byte TAG_FEDAUTH = 0x06;
        final byte TAG_TERMINATOR = (byte) 0xFF;

        // Prepare value fields
        byte[] versionBytes = version != null ? version.toBytes() : new byte[6];
        byte[] encryptionBytes = new byte[] { encryption != null ? encryption.getValue() : 0 };
        byte[] instanceBytes = new byte[] { instance };
        byte[] threadIdBytes = new byte[4];
        threadIdBytes[0] = (byte) (threadId & 0xFF);
        threadIdBytes[1] = (byte) ((threadId >> 8) & 0xFF);
        threadIdBytes[2] = (byte) ((threadId >> 16) & 0xFF);
        threadIdBytes[3] = (byte) ((threadId >> 24) & 0xFF);
        byte[] marsBytes = new byte[] { mars };
        byte[] fedAuthBytes = new byte[] { fedAuth };

        // Calculate offsets
        int offset = 0;
        int[][] fields = new int[6][3]; // tag, offset, length
        int pos = 0;
        fields[pos++] = new int[] { TAG_VERSION, offset, versionBytes.length };
        offset += versionBytes.length;
        fields[pos++] = new int[] { TAG_ENCRYPTION, offset, encryptionBytes.length };
        offset += encryptionBytes.length;
        fields[pos++] = new int[] { TAG_INSTANCE, offset, instanceBytes.length };
        offset += instanceBytes.length;
        fields[pos++] = new int[] { TAG_THREADID, offset, threadIdBytes.length };
        offset += threadIdBytes.length;
        fields[pos++] = new int[] { TAG_MARS, offset, marsBytes.length };
        offset += marsBytes.length;
        fields[pos++] = new int[] { TAG_FEDAUTH, offset, fedAuthBytes.length };
        offset += fedAuthBytes.length;

        int headerLen = 5 * fields.length + 1; // 5 bytes per field + 1 for terminator
        int totalLen = headerLen + offset;
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(totalLen);

        // Write header
        for (int[] f : fields) {
            buf.put((byte) f[0]);
            buf.putShort((short) (headerLen + f[1]));
            buf.putShort((short) f[2]);
        }
        buf.put(TAG_TERMINATOR);

        // Write values
        buf.put(versionBytes);
        buf.put(encryptionBytes);
        buf.put(instanceBytes);
        buf.put(threadIdBytes);
        buf.put(marsBytes);
        buf.put(fedAuthBytes);

        buf.flip();
        this.buffer = buf;
    }

    private void extractBufferData() {
        // Parse the buffer to extract PreLogin fields according to TDS spec.
        if (buffer == null) return;
        buffer.mark();
        try {
            // Read header: tag, offset, length until terminator (0xFF)
            java.util.Map<Byte, Integer> offsets = new java.util.HashMap<>();
            java.util.Map<Byte, Integer> lengths = new java.util.HashMap<>();
            int headerEnd = -1;
            while (buffer.remaining() >= 5) {
                byte tag = buffer.get();
                if ((tag & 0xFF) == 0xFF) { headerEnd = buffer.position(); break; }
                int offset = buffer.getShort() & 0xFFFF;
                int length = buffer.getShort() & 0xFFFF;
                offsets.put(tag, offset);
                lengths.put(tag, length);
            }
            if (headerEnd < 0) return;
            // Extract fields
            for (java.util.Map.Entry<Byte, Integer> entry : offsets.entrySet()) {
                byte tag = entry.getKey();
                int off = entry.getValue();
                int len = lengths.get(tag);
                int oldPos = buffer.position();
                buffer.position(off);
                switch (tag) {
                    case 0x00: // Version
                        if (len == 6) {
                            byte[] v = new byte[6]; buffer.get(v);
                            this.version = SqlVersion.fromBytes(v);
                        }
                        break;
                    case 0x01: // Encryption
                        if (len == 1) this.encryption = EncryptionType.fromValue(buffer.get());
                        break;
                    case 0x02: // Instance
                        if (len == 1) this.instance = buffer.get();
                        break;
                    case 0x03: // ThreadId
                        if (len == 4) {
                            int tid = buffer.getInt();
                            this.threadId = tid & 0xFFFFFFFFL;
                        }
                        break;
                    case 0x04: // Mars
                        if (len == 1) this.mars = buffer.get();
                        break;
                    case 0x06: // FedAuth
                        if (len == 1) this.fedAuth = buffer.get();
                        break;
                }
                buffer.position(oldPos);
            }
        } finally {
            buffer.reset();
        }
    }
}