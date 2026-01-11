// java
// File: `src/main/java/org/tdslib/javatdslib/payloads/prelogin/SqlVersion.java`

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.payloads.prelogin;

/**
 * SQL version.
 */
public class SqlVersion {
    private final byte major;
    private final byte minor;
    private final short build;
    private final short revision;

    /**
     * Get major version.
     *
     * @return major version byte
     */
    public byte getMajor() {
        return major;
    }

    /**
     * Get minor version.
     *
     * @return minor version byte
     */
    public byte getMinor() {
        return minor;
    }

    /**
     * Get build number.
     *
     * @return build number as a short
     */
    public short getBuild() {
        return build;
    }

    /**
     * Get revision.
     *
     * @return revision as a short
     */
    public short getRevision() {
        return revision;
    }

    // --- Constructors (grouped) ---

    /**
     * Construct with explicitly typed values.
     *
     * @param major    major version (0..255)
     * @param minor    minor version (0..255)
     * @param build    build number (0..65535)
     * @param revision revision (0..65535)
     */
    public SqlVersion(final byte major, final byte minor, final int build, final int revision) {
        this.major = major;
        this.minor = minor;
        if (build < 0 || build > 0xFFFF) {
            throw new IllegalArgumentException("build out of range: " + build);
        }
        if (revision < 0 || revision > 0xFFFF) {
            throw new IllegalArgumentException("revision out of range: " + revision);
        }
        this.build = (short) (build & 0xFFFF);
        this.revision = (short) (revision & 0xFFFF);
    }

    /**
     * Convenience constructor using int for major/minor.
     *
     * @param major    major version (0..255)
     * @param minor    minor version (0..255)
     * @param build    build number (0..65535)
     * @param revision revision (0..65535)
     */
    public SqlVersion(final int major, final int minor, final int build, final int revision) {
        this((byte) (major & 0xFF), (byte) (minor & 0xFF), build, revision);
    }

    /**
     * Construct from two bytes representing the build (little-endian).
     * Revision will be zero.
     *
     * @param major     major version
     * @param minor     minor version
     * @param buildLo   low byte of build
     * @param buildHi   high byte of build
     */
    public SqlVersion(final byte major, final byte minor, final byte buildLo, final byte buildHi) {
        this.major = major;
        this.minor = minor;
        this.build = (short) (((buildHi & 0xFF) << 8) | (buildLo & 0xFF));
        this.revision = 0;
    }

    /**
     * Construct from six bytes: major, minor, buildLo, buildHi, revLo, revHi.
     * All multi-byte fields are interpreted little-endian.
     *
     * @param b6 six-byte array as produced by {@link #toBytes()}
     */
    public SqlVersion(final byte[] b6) {
        if (b6 == null || b6.length < 6) {
            throw new IllegalArgumentException("byte array must be at least 6 bytes");
        }
        this.major = b6[0];
        this.minor = b6[1];
        this.build = (short) (((b6[3] & 0xFF) << 8) | (b6[2] & 0xFF));
        this.revision = (short) (((b6[5] & 0xFF) << 8) | (b6[4] & 0xFF));
    }

    /**
     * Serialize to 6 bytes per TDS spec.
     * Layout (little-endian for 2-byte fields):
     * [0] = major
     * [1] = minor
     * [2] = build low
     * [3] = build high
     * [4] = revision low
     * [5] = revision high
     *
     * @return 6 byte representation
     */
    public byte[] toBytes() {
        byte[] b = new byte[6];
        b[0] = major;
        b[1] = minor;
        b[2] = (byte) (build & 0xFF);
        b[3] = (byte) ((build >> 8) & 0xFF);
        b[4] = (byte) (revision & 0xFF);
        b[5] = (byte) ((revision >> 8) & 0xFF);
        return b;
    }

    /**
     * Parse from 6 bytes.
     *
     * @param b six-byte array
     * @return SqlVersion parsed
     */
    public static SqlVersion fromBytes(final byte[] b) {
        return new SqlVersion(b);
    }

    @Override
    public String toString() {
        int maj = major & 0xFF;
        int min = minor & 0xFF;
        int bld = build & 0xFFFF;
        int rev = revision & 0xFFFF;
        return maj + "." + min + "." + bld + "." + rev;
    }
}
