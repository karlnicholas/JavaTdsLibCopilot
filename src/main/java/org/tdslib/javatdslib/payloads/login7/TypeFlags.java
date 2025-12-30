// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package org.tdslib.javatdslib.payloads.login7;

public final class TypeFlags {
    public enum OptionSqlType { Default, TSQL }
    public enum OptionOleDb { Off, On }
    public enum OptionAccessIntent { ReadWrite, ReadOnly }

    private static final int OptionSqlTypeBitIndex = 0x08;
    private static final int OptionOleDbBitIndex = 0x10;
    private static final int OptionAccesIntentBitIndex = 0x20;

    private byte value;

    public TypeFlags() {
        this.value = 0;
        setSqlType(OptionSqlType.Default);
        setOleDb(OptionOleDb.Off);
        setAccessIntent(OptionAccessIntent.ReadWrite);
    }

    public TypeFlags(byte value) { this.value = value; }

    public OptionSqlType getSqlType() {
        if ((value & OptionSqlTypeBitIndex) == OptionSqlTypeBitIndex) {
            return OptionSqlType.TSQL;
        }
        return OptionSqlType.Default;
    }

    public void setSqlType(OptionSqlType t) {
        if (t == OptionSqlType.Default) {
            value &= (byte) (0xFF - OptionSqlTypeBitIndex);
        } else {
            value |= OptionSqlTypeBitIndex;
        }
    }

    public OptionOleDb getOleDb() {
        if ((value & OptionOleDbBitIndex) == OptionOleDbBitIndex) {
            return OptionOleDb.On;
        }
        return OptionOleDb.Off;
    }

    public void setOleDb(OptionOleDb v) {
        if (v == OptionOleDb.Off) {
            value &= (byte) (0xFF - OptionOleDbBitIndex);
        } else {
            value |= OptionOleDbBitIndex;
        }
    }

    public OptionAccessIntent getAccessIntent() {
        if ((value & OptionAccesIntentBitIndex) == OptionAccesIntentBitIndex) {
            return OptionAccessIntent.ReadOnly;
        }
        return OptionAccessIntent.ReadWrite;
    }

    public void setAccessIntent(OptionAccessIntent v) {
        if (v == OptionAccessIntent.ReadWrite) {
            value &= (byte) (0xFF - OptionAccesIntentBitIndex);
        } else {
            value |= OptionAccesIntentBitIndex;
        }
    }

    public byte toByte() { return value; }

    public static TypeFlags fromByte(byte b) { return new TypeFlags(b); }

    @Override
    public String toString() {
        return String.format("TypeFlags[value=0x%02X, SqlType=%s, OleDb=%s, AccessIntent=%s]",
            Byte.toUnsignedInt(value), getSqlType(), getOleDb(), getAccessIntent());
    }
}
