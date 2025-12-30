// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.payloads.prelogin;

/**
 * SQL version.
 */
public class SqlVersion {
    private final byte major;
    private final byte minor;
    private final short build;
    private final byte revision;

    /**
     * Major version.
     */
    public byte getMajor() {
        return major;
    }

    /**
     * Minor version.
     */
    public byte getMinor() {
        return minor;
    }

    /**
     * Build number.
     */
    public short getBuild() {
        return build;
    }

    /**
     * Revision.
     */
    public byte getRevision() {
        return revision;
    }

    /**
     * Create a new instance.
     */

    public SqlVersion(byte major, byte minor, int build, byte revision) {
        this.major = major;
        this.minor = minor;
        this.build = (short) build;
        this.revision = revision;
    }

    // Add constructor for (int, int, int, int)
    public SqlVersion(int major, int minor, int build, int revision) {
        this((byte) major, (byte) minor, build, (byte) revision);
    }

    // Serialize to 6 bytes per TDS spec
    public byte[] toBytes() {
        byte[] b = new byte[6];
        b[0] = major;
        b[1] = minor;
        b[2] = (byte) ((build >> 8) & 0xFF);
        b[3] = (byte) (build & 0xFF);
        b[4] = revision;
        b[5] = 0; // unused
        return b;
    }

    // Parse from 6 bytes
    public static SqlVersion fromBytes(byte[] b) {
        if (b == null || b.length < 4) return new SqlVersion((byte)0, (byte)0, 0, (byte)0);
        byte major = b[0];
        byte minor = b[1];
        int build = ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
        byte revision = b.length > 4 ? b[4] : 0;
        return new SqlVersion(major, minor, build, revision);
    }

    /**
     * Create a new instance.
     */
    public SqlVersion(byte major, byte minor, byte buildHi, byte buildLow) {
        this(major, minor, (buildHi << 8) | buildLow, (byte) 0);
    }

    @Override
    public String toString() {
        return major + "." + minor + "." + build + "." + revision;
    }
}