package org.tdslib.javatdslib.tokens.loginack;

// TdsVersion.java
public class TdsVersion {
    private final int major;
    private final int minor;
    private final int build;

    public TdsVersion(int major, int minor, int build) {
        this.major = major;
        this.minor = minor;
        this.build = build;
    }

    @Override
    public String toString() {
        return major + "." + minor + "." + build;
    }
}