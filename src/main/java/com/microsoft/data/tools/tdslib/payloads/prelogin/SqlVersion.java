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