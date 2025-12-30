package com.microsoft.data.tools.tdslib.payloads.login7.auth;

public enum ADALWorkflow {
    UserPass((byte)0x01),
    Integrated((byte)0x02);

    private final byte value;

    ADALWorkflow(byte v) { this.value = v; }
    public byte getValue() { return value; }
}
