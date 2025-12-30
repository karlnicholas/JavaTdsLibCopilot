// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.loginack;

/**
 * Program version.
 */
public class ProgVersion {
    private final byte major;
    private final byte minor;
    private final int build;

    /**
     * Major.
     */
    public byte getMajor() {
        return major;
    }

    /**
     * Minor.
     */
    public byte getMinor() {
        return minor;
    }

    /**
     * Build revision.
     */
    public int getBuild() {
        return build;
    }

    /**
     * Create a new instance.
     */
    public ProgVersion(byte major, byte minor, int build) {
        this.major = major;
        this.minor = minor;
        this.build = build;
    }

    /**
     * Create a new instance.
     */
    public ProgVersion(byte major, byte minor, byte buildHi, byte buildLow) {
        this(major, minor, (buildHi << 8) | buildLow);
    }

    /**
     * Gets a human readable string representation of this object.
     */
    @Override
    public String toString() {
        return "ProgVersion[Major=" + major + ", Minor=" + minor + ", Build=" + build + "]";
    }
}