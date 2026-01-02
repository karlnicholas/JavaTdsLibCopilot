// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.payloads.login7;

/**
 * Represents the OptionFlags1 byte in the TDS LOGIN7 message.
 * See MS-TDS specification section 2.2.6.4 for bit meanings.
 */
public final class OptionFlags1 {

    // Bit masks according to MS-TDS
    private static final int BIT_ENDIAN         = 0x01;  // 1 = LittleEndian (x86), 0 = BigEndian (68000)
    private static final int BIT_CHARSET        = 0x02;  // 1 = EBCDIC, 0 = ASCII
    private static final int BIT_FLOAT_VAX      = 0x04;  // VAX floating point
    private static final int BIT_FLOAT_ND5000   = 0x08;  // ND5000 floating point
    private static final int BIT_BCP_DUMPLOAD   = 0x10;  // 1 = BCP/Dumpload ON
    private static final int BIT_USE_DB         = 0x20;  // 1 = UseDB ON
    private static final int BIT_INIT_DB_FATAL  = 0x40;  // 1 = Fatal on init db change, 0 = Warn
    private static final int BIT_SET_LANG_WARN  = 0x80;  // 1 = Warn on language change

    public enum Endianness { LittleEndian, BigEndian }
    public enum Charset    { Ascii, Ebcdic }
    public enum FloatType  { IEEE, VAX, ND5000 }
    public enum BcpDumpload{ On, Off }
    public enum UseDb      { On, Off }
    public enum InitDb     { Warn, Fatal }
    public enum LangWarn   { On, Off }

    private byte value;

    /**
     * Default constructor - produces a value typical for modern clients
     * (little-endian, ASCII, IEEE float, BCP on, etc.)
     * This usually results in 0xF0 or 0xE0 — both work well with SQL Server 2022.
     */
    public OptionFlags1() {
        this.value = (byte) 0xE0;  // Preferred safe default for simple logins

        // Or explicitly set (same result):
        // setEndian(Endianness.LittleEndian);
        // setCharset(Charset.Ascii);
        // setFloat(FloatType.IEEE);
        // setBcpDumpload(BcpDumpload.On);
        // setUseDb(UseDb.Off);
        // setInitDb(InitDb.Warn);
        // setLangWarn(LangWarn.On);
    }

    public OptionFlags1(byte value) {
        this.value = value;
    }

    public Endianness getEndianness() {
        return (value & BIT_ENDIAN) != 0 ? Endianness.LittleEndian : Endianness.BigEndian;
    }

    public void setEndianness(Endianness e) {
        if (e == Endianness.LittleEndian) {
            value |= BIT_ENDIAN;
        } else {
            value &= ~BIT_ENDIAN;
        }
    }

    public Charset getCharset() {
        return (value & BIT_CHARSET) != 0 ? Charset.Ebcdic : Charset.Ascii;
    }

    public void setCharset(Charset c) {
        if (c == Charset.Ebcdic) {
            value |= BIT_CHARSET;
        } else {
            value &= ~BIT_CHARSET;
        }
    }

    public FloatType getFloat() {
        if ((value & BIT_FLOAT_VAX) != 0) return FloatType.VAX;
        if ((value & BIT_FLOAT_ND5000) != 0) return FloatType.ND5000;
        return FloatType.IEEE;
    }

    public void setFloat(FloatType f) {
        value &= ~(BIT_FLOAT_VAX | BIT_FLOAT_ND5000);
        if (f == FloatType.VAX)     value |= BIT_FLOAT_VAX;
        else if (f == FloatType.ND5000) value |= BIT_FLOAT_ND5000;
    }

    public BcpDumpload getBcpDumpload() {
        return (value & BIT_BCP_DUMPLOAD) != 0 ? BcpDumpload.On : BcpDumpload.Off;
    }

    public void setBcpDumpload(BcpDumpload v) {
        if (v == BcpDumpload.On) {
            value |= BIT_BCP_DUMPLOAD;
        } else {
            value &= ~BIT_BCP_DUMPLOAD;
        }
    }

    public UseDb getUseDb() {
        return (value & BIT_USE_DB) != 0 ? UseDb.On : UseDb.Off;
    }

    public void setUseDb(UseDb v) {
        if (v == UseDb.On) {
            value |= BIT_USE_DB;
        } else {
            value &= ~BIT_USE_DB;
        }
    }

    public InitDb getInitDb() {
        return (value & BIT_INIT_DB_FATAL) != 0 ? InitDb.Fatal : InitDb.Warn;
    }

    public void setInitDb(InitDb v) {
        if (v == InitDb.Fatal) {
            value |= BIT_INIT_DB_FATAL;
        } else {
            value &= ~BIT_INIT_DB_FATAL;
        }
    }

    public LangWarn getLangWarn() {
        return (value & BIT_SET_LANG_WARN) != 0 ? LangWarn.On : LangWarn.Off;
    }

    public void setLangWarn(LangWarn v) {
        if (v == LangWarn.On) {
            value |= BIT_SET_LANG_WARN;
        } else {
            value &= ~BIT_SET_LANG_WARN;
        }
    }

    public byte toByte() {
        return value;
    }

    public static OptionFlags1 fromByte(byte b) {
        return new OptionFlags1(b);
    }

    @Override
    public String toString() {
        return String.format(
                "OptionFlags1[0x%02X] (Endian=%s, Charset=%s, Float=%s, BCP=%s, UseDB=%s, InitDB=%s, LangWarn=%s)",
                Byte.toUnsignedInt(value),
                getEndianness(),
                getCharset(),
                getFloat(),
                getBcpDumpload(),
                getUseDb(),
                getInitDb(),
                getLangWarn()
        );
    }
}