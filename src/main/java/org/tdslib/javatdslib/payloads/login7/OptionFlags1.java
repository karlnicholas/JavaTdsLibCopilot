// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.payloads.login7;

// === Package-private enums (allowed in same file as the public class) ===

enum OptionEndian {
    LittleEndian, BigEndian
}

enum OptionCharset {
    Ascii, Ebcdic
}

enum OptionFloat {
    IEEE, VAX, ND5000
}

enum OptionBcpDumpload {
    On, Off
}

enum OptionUseDb {
    On, Off
}

enum OptionInitDb {
    Warn, Fatal
}

enum OptionLangWarn {
    On, Off
}

/**
 * TDS Login7 OptionFlags1
 * Default value set to 0xE0 as requested
 */
public final class OptionFlags1 {

    private static final int OPTION_ENDIAN_BIT        = 0x01;
    private static final int OPTION_CHARSET_BIT       = 0x02;
    private static final int OPTION_FLOAT_VAX_BIT     = 0x04;
    private static final int OPTION_FLOAT_ND5000_BIT  = 0x08;
    private static final int OPTION_BCP_DUMPLOAD_BIT  = 0x10;
    private static final int OPTION_USE_DB_BIT        = 0x20;
    private static final int OPTION_INIT_DB_BIT       = 0x40;
    private static final int OPTION_LANG_WARN_BIT     = (byte) 0x80;

    private byte value;

    // ── Getters ─────────────────────────────────────────────────────────────

    public byte getValue() {
        return value;
    }

    public OptionEndian getEndian() {
        return (value & OPTION_ENDIAN_BIT) != 0 ? OptionEndian.BigEndian : OptionEndian.LittleEndian;
    }

    public OptionCharset getCharset() {
        return (value & OPTION_CHARSET_BIT) != 0 ? OptionCharset.Ebcdic : OptionCharset.Ascii;
    }

    public OptionFloat getFloat() {
        if ((value & OPTION_FLOAT_VAX_BIT) != 0)    return OptionFloat.VAX;
        if ((value & OPTION_FLOAT_ND5000_BIT) != 0) return OptionFloat.ND5000;
        return OptionFloat.IEEE;
    }

    public OptionBcpDumpload getBcpDumpload() {
        return (value & OPTION_BCP_DUMPLOAD_BIT) != 0 ? OptionBcpDumpload.Off : OptionBcpDumpload.On;
    }

    public OptionUseDb getUseDb() {
        return (value & OPTION_USE_DB_BIT) != 0 ? OptionUseDb.Off : OptionUseDb.On;
    }

    public OptionInitDb getInitDb() {
        return (value & OPTION_INIT_DB_BIT) != 0 ? OptionInitDb.Fatal : OptionInitDb.Warn;
    }

    public OptionLangWarn getLangWarn() {
        return (value & OPTION_LANG_WARN_BIT) != 0 ? OptionLangWarn.On : OptionLangWarn.Off;
    }

    // ── Setters ─────────────────────────────────────────────────────────────

    public void setEndian(OptionEndian v) {
        if (v == OptionEndian.LittleEndian) value &= ~OPTION_ENDIAN_BIT;
        else                                value |=  OPTION_ENDIAN_BIT;
    }

    public void setCharset(OptionCharset v) {
        if (v == OptionCharset.Ascii) value &= ~OPTION_CHARSET_BIT;
        else                          value |=  OPTION_CHARSET_BIT;
    }

    public void setFloat(OptionFloat v) {
        value &= ~(OPTION_FLOAT_VAX_BIT | OPTION_FLOAT_ND5000_BIT);
        if (v == OptionFloat.VAX)    value |= OPTION_FLOAT_VAX_BIT;
        if (v == OptionFloat.ND5000) value |= OPTION_FLOAT_ND5000_BIT;
    }

    public void setBcpDumpload(OptionBcpDumpload v) {
        if (v == OptionBcpDumpload.On) value &= ~OPTION_BCP_DUMPLOAD_BIT;
        else                           value |=  OPTION_BCP_DUMPLOAD_BIT;
    }

    public void setUseDb(OptionUseDb v) {
        if (v == OptionUseDb.On) value &= ~OPTION_USE_DB_BIT;
        else                     value |=  OPTION_USE_DB_BIT;
    }

    public void setInitDb(OptionInitDb v) {
        if (v == OptionInitDb.Warn) value &= ~OPTION_INIT_DB_BIT;
        else                        value |=  OPTION_INIT_DB_BIT;
    }

    public void setLangWarn(OptionLangWarn v) {
        if (v == OptionLangWarn.Off) value &= ~OPTION_LANG_WARN_BIT;
        else                         value |=  OPTION_LANG_WARN_BIT;
    }

    // ── Constructors ────────────────────────────────────────────────────────

    public OptionFlags1() {
        this.value = (byte) 0xE0;   // ← default value you wanted
    }

    public OptionFlags1(byte value) {
        this.value = value;
    }

    // ── Utilities ───────────────────────────────────────────────────────────

    public byte toByte() {
        return value;
    }

    public static OptionFlags1 fromByte(byte value) {
        return new OptionFlags1(value);
    }

    @Override
    public String toString() {
        String bin = String.format("%8s", Integer.toBinaryString(Byte.toUnsignedInt(value)))
                .replace(' ', '0');

        return "OptionFlags1[0x%02X | 0b%s] (Endian=%s, Charset=%s, Float=%s, Bcp=%s, UseDb=%s, InitDb=%s, LangWarn=%s)"
                .formatted(
                        Byte.toUnsignedInt(value),
                        bin,
                        getEndian(),
                        getCharset(),
                        getFloat(),
                        getBcpDumpload(),
                        getUseDb(),
                        getInitDb(),
                        getLangWarn()
                );
    }

    // Quick test main method
    public static void main(String[] args) {
        OptionFlags1 flags = new OptionFlags1();
        System.out.println(flags);

        flags.setLangWarn(OptionLangWarn.Off);
        flags.setBcpDumpload(OptionBcpDumpload.Off);
        System.out.println(flags);
    }
}