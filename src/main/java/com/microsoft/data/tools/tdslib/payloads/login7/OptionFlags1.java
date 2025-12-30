// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.payloads.login7;

public final class OptionFlags1 {
    public enum OptionEndian { LittleEndian, BigEndian }
    public enum OptionCharset { Ascii, Ebcdic }
    public enum OptionFloat { IEEE, VAX, ND5000 }
    public enum OptionBcpDumpload { On, Off }
    public enum OptionUseDb { On, Off }
    public enum OptionInitDb { Warn, Fatal }
    public enum OptionLangWarn { On, Off }

    private static final int OptionEndianBitIndex = 0x01;
    private static final int OptionCharsetBitIndex = 0x02;
    private static final int OptionFloatBitIndexVax = 0x04;
    private static final int OptionFloatBitIndexND5000 = 0x08;
    private static final int OptionBcpDumploadBitIndex = 0x10;
    private static final int OptionUseDbBitIndex = 0x20;
    private static final int OptionIndexDbBitIndex = 0x40;
    private static final int OptionLangWarnBitIndex = 0x80;

    private byte value;

    public OptionFlags1() {
        setEndian(OptionEndian.LittleEndian);
        setCharset(OptionCharset.Ascii);
        setFloat(OptionFloat.IEEE);
        setBcpDumpload(OptionBcpDumpload.Off);
        setUseDb(OptionUseDb.Off);
        setLangWarn(OptionLangWarn.On);
        setInitDb(OptionInitDb.Warn);
    }

    public OptionFlags1(byte value) { this.value = value; }

    public OptionEndian getEndian() {
        if ((value & OptionEndianBitIndex) == OptionEndianBitIndex) return OptionEndian.BigEndian;
        return OptionEndian.LittleEndian;
    }

    public void setEndian(OptionEndian e) {
        if (e == OptionEndian.LittleEndian) value &= (byte) (0xFF - OptionEndianBitIndex);
        else value |= OptionEndianBitIndex;
    }

    public OptionCharset getCharset() {
        if ((value & OptionCharsetBitIndex) == OptionCharsetBitIndex) return OptionCharset.Ebcdic;
        return OptionCharset.Ascii;
    }

    public void setCharset(OptionCharset c) {
        if (c == OptionCharset.Ascii) value &= (byte) (0xFF - OptionCharsetBitIndex);
        else value |= OptionCharsetBitIndex;
    }

    public OptionFloat getFloat() {
        if ((value & OptionFloatBitIndexVax) == OptionFloatBitIndexVax) return OptionFloat.VAX;
        else if ((value & OptionFloatBitIndexND5000) == OptionFloatBitIndexND5000) return OptionFloat.ND5000;
        return OptionFloat.IEEE;
    }

    public void setFloat(OptionFloat f) {
        if (f == OptionFloat.IEEE) {
            value &= (byte) (0xFF - OptionFloatBitIndexVax);
            value &= (byte) (0xFF - OptionFloatBitIndexND5000);
        } else if (f == OptionFloat.VAX) {
            value |= OptionFloatBitIndexVax;
            value &= (byte) (0xFF - OptionFloatBitIndexND5000);
        } else {
            value &= (byte) (0xFF - OptionFloatBitIndexVax);
            value |= OptionFloatBitIndexND5000;
        }
    }

    public OptionBcpDumpload getBcpDumpload() {
        if ((value & OptionBcpDumploadBitIndex) == OptionBcpDumploadBitIndex) return OptionBcpDumpload.Off;
        return OptionBcpDumpload.On;
    }

    public void setBcpDumpload(OptionBcpDumpload v) {
        if (v == OptionBcpDumpload.On) value &= (byte) (0xFF - OptionBcpDumploadBitIndex);
        else value |= OptionBcpDumploadBitIndex;
    }

    public OptionUseDb getUseDb() {
        if ((value & OptionUseDbBitIndex) == OptionUseDbBitIndex) return OptionUseDb.Off;
        return OptionUseDb.On;
    }

    public void setUseDb(OptionUseDb v) {
        if (v == OptionUseDb.On) value &= (byte) (0xFF - OptionUseDbBitIndex);
        else value |= OptionUseDbBitIndex;
    }

    public OptionInitDb getInitDb() {
        if ((value & OptionIndexDbBitIndex) == OptionIndexDbBitIndex) return OptionInitDb.Fatal;
        return OptionInitDb.Warn;
    }

    public void setInitDb(OptionInitDb v) {
        if (v == OptionInitDb.Warn) value &= (byte) (0xFF - OptionIndexDbBitIndex);
        else value |= OptionIndexDbBitIndex;
    }

    public OptionLangWarn getLangWarn() {
        if ((value & OptionLangWarnBitIndex) == OptionLangWarnBitIndex) return OptionLangWarn.On;
        return OptionLangWarn.Off;
    }

    public void setLangWarn(OptionLangWarn v) {
        if (v == OptionLangWarn.Off) value &= (byte) (0xFF - OptionLangWarnBitIndex);
        else value |= OptionLangWarnBitIndex;
    }

    public byte toByte() { return value; }

    public static OptionFlags1 fromByte(byte b) { return new OptionFlags1(b); }

    @Override
    public String toString() {
        return String.format("OptionFlags1[value=0x%02X, Endian=%s, Charset=%s, Float=%s, BcpDumpload=%s, UseDb=%s, InitDb=%s, LangWarn=%s]",
            Byte.toUnsignedInt(value), getEndian(), getCharset(), getFloat(), getBcpDumpload(), getUseDb(), getInitDb(), getLangWarn());
    }
}
