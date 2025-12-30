// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.payloads.login7;

public final class OptionFlags2 {
    public enum OptionInitLang { Warn, Fatal }
    public enum OptionOdbc { Off, On }
    public enum OptionUser { Normal, Server, RemUser, SqlRepl }
    public enum OptionIntegratedSecurity { Off, On }

    private static final int OptionInitLangBitIndex = 0x01;
    private static final int OptionOdbcBitIndex = 0x02;
    private static final int OptionUserBitIndexServer = 0x10;
    private static final int OptionUserBitIndexRemUser = 0x20;
    private static final int OptionUserBitIndexSqlRepl = 0x40;
    private static final int OptionIntegratedSecurityBitIndex = 0x80;

    private byte value;

    public OptionFlags2() {
        setInitLang(OptionInitLang.Warn);
        setOdbc(OptionOdbc.Off);
        setUser(OptionUser.Normal);
        setIntegratedSecurity(OptionIntegratedSecurity.Off);
    }

    public OptionFlags2(byte value) { this.value = value; }

    public OptionInitLang getInitLang() {
        if ((value & OptionInitLangBitIndex) == OptionInitLangBitIndex) return OptionInitLang.Fatal;
        return OptionInitLang.Warn;
    }

    public void setInitLang(OptionInitLang v) {
        if (v == OptionInitLang.Warn) value &= (byte) (0xFF - OptionInitLangBitIndex);
        else value |= OptionInitLangBitIndex;
    }

    public OptionOdbc getOdbc() {
        if ((value & OptionOdbcBitIndex) == OptionOdbcBitIndex) return OptionOdbc.On;
        return OptionOdbc.Off;
    }

    public void setOdbc(OptionOdbc v) {
        if (v == OptionOdbc.Off) value &= (byte) (0xFF - OptionOdbcBitIndex);
        else value |= OptionOdbcBitIndex;
    }

    public OptionUser getUser() {
        if ((value & OptionUserBitIndexServer) == OptionUserBitIndexServer) return OptionUser.Server;
        if ((value & OptionUserBitIndexRemUser) == OptionUserBitIndexRemUser) return OptionUser.RemUser;
        if ((value & OptionUserBitIndexSqlRepl) == OptionUserBitIndexSqlRepl) return OptionUser.SqlRepl;
        return OptionUser.Normal;
    }

    public void setUser(OptionUser u) {
        if (u == OptionUser.Normal) {
            value &= (byte) (0xFF - OptionUserBitIndexServer);
            value &= (byte) (0xFF - OptionUserBitIndexRemUser);
            value &= (byte) (0xFF - OptionUserBitIndexSqlRepl);
        } else if (u == OptionUser.Server) {
            value |= OptionUserBitIndexServer;
            value &= (byte) (0xFF - OptionUserBitIndexRemUser);
            value &= (byte) (0xFF - OptionUserBitIndexSqlRepl);
        } else if (u == OptionUser.RemUser) {
            value &= (byte) (0xFF - OptionUserBitIndexServer);
            value |= OptionUserBitIndexRemUser;
            value &= (byte) (0xFF - OptionUserBitIndexSqlRepl);
        } else {
            value &= (byte) (0xFF - OptionUserBitIndexServer);
            value &= (byte) (0xFF - OptionUserBitIndexRemUser);
            value |= OptionUserBitIndexSqlRepl;
        }
    }

    public OptionIntegratedSecurity getIntegratedSecurity() {
        if ((value & OptionIntegratedSecurityBitIndex) == OptionIntegratedSecurityBitIndex) return OptionIntegratedSecurity.On;
        return OptionIntegratedSecurity.Off;
    }

    public void setIntegratedSecurity(OptionIntegratedSecurity v) {
        if (v == OptionIntegratedSecurity.Off) value &= (byte) (0xFF - OptionIntegratedSecurityBitIndex);
        else value |= OptionIntegratedSecurityBitIndex;
    }

    public byte toByte() { return value; }
    public static OptionFlags2 fromByte(byte b) { return new OptionFlags2(b); }

    @Override
    public String toString() {
        return String.format("OptionFlags2[value=0x%02X, InitLang=%s, ODBC=%s, User=%s, IntegratedSecurity=%s]",
            Byte.toUnsignedInt(value), getInitLang(), getOdbc(), getUser(), getIntegratedSecurity());
    }
}
