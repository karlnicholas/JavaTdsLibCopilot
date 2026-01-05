// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.tokens.loginack;

// ProgVersion.java
public class ProgVersion {
    private final int major;
    private final int minor;
    private final int build;
    private final int subBuild;

    public ProgVersion(int major, int minor, int build, int subBuild) {
        this.major = major;
        this.minor = minor;
        this.build = build;
        this.subBuild = subBuild;
    }

    @Override
    public String toString() {
        return major + "." + minor + "." + build + "." + subBuild;
    }
}