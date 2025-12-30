// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.data.tools.tdslib.payloads.login7;

public final class OptionFlags3 {
    public enum OptionChangePassword { No, Yes }

    private static final int OptionChangePasswordBitIndex = 0x01;
    private static final int OptionBinaryXmlBitIndex = 0x02;
    private static final int OptionSpawnUserInstanceBitIndex = 0x04;
    private static final int OptionUnkownCollationHandlingBitIndex = 0x08;
    private static final int OptionExtensionUsedBitIndex = 0x10;

    private byte value;

    public OptionFlags3() {
        setChangePassword(OptionChangePassword.No);
        setBinaryXml(false);
        setSpawnUserInstance(false);
        setUnknownCollationHandling(true);
        setExtensionUsed(true);
    }

    public OptionFlags3(byte value) { this.value = value; }

    public OptionChangePassword getChangePassword() {
        if ((value & OptionChangePasswordBitIndex) == OptionChangePasswordBitIndex) return OptionChangePassword.Yes;
        return OptionChangePassword.No;
    }

    public void setChangePassword(OptionChangePassword v) {
        if (v == OptionChangePassword.No) value &= (byte) (0xFF - OptionChangePasswordBitIndex);
        else value |= OptionChangePasswordBitIndex;
    }

    public boolean isBinaryXml() { return (value & OptionBinaryXmlBitIndex) == OptionBinaryXmlBitIndex; }
    public void setBinaryXml(boolean v) { if (v) value |= OptionBinaryXmlBitIndex; else value &= (byte) (0xFF - OptionBinaryXmlBitIndex); }

    public boolean isSpawnUserInstance() { return (value & OptionSpawnUserInstanceBitIndex) == OptionSpawnUserInstanceBitIndex; }
    public void setSpawnUserInstance(boolean v) { if (v) value |= OptionSpawnUserInstanceBitIndex; else value &= (byte) (0xFF - OptionSpawnUserInstanceBitIndex); }

    public boolean isUnknownCollationHandling() { return (value & OptionUnkownCollationHandlingBitIndex) == OptionUnkownCollationHandlingBitIndex; }
    public void setUnknownCollationHandling(boolean v) { if (v) value |= OptionUnkownCollationHandlingBitIndex; else value &= (byte) (0xFF - OptionUnkownCollationHandlingBitIndex); }

    public boolean isExtensionUsed() { return (value & OptionExtensionUsedBitIndex) == OptionExtensionUsedBitIndex; }
    public void setExtensionUsed(boolean v) { if (v) value |= OptionExtensionUsedBitIndex; else value &= (byte) (0xFF - OptionExtensionUsedBitIndex); }

    public byte toByte() { return value; }
    public static OptionFlags3 fromByte(byte b) { return new OptionFlags3(b); }

    @Override
    public String toString() {
        return String.format("OptionFlags3[value=0x%02X, ChangePassword=%s, BinaryXml=%s, SpawnUserInstance=%s, UnknownCollationHandling=%s, ExtensionUsed=%s]",
            Byte.toUnsignedInt(value), getChangePassword(), isBinaryXml(), isSpawnUserInstance(), isUnknownCollationHandling(), isExtensionUsed());
    }
}
