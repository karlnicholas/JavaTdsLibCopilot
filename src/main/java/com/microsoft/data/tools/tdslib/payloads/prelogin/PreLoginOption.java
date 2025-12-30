package com.microsoft.data.tools.tdslib.payloads.prelogin;

/**
 * PreLogin option tags for TDS PreLogin payload.
 */
public final class PreLoginOption {
    public static final byte VERSION = 0x00;
    public static final byte ENCRYPTION = 0x01;
    public static final byte INSTANCE = 0x02;
    public static final byte THREADID = 0x03;
    public static final byte MARS = 0x04;
    public static final byte FEDAUTH = 0x06;
    public static final byte TERMINATOR = (byte)0xFF;
    private PreLoginOption() {}
}
