package org.tdslib.javatdslib.tokens.done;

/**
 * Status flags for DONE family tokens.
 */
public enum DoneStatus {
    FINAL(0x0000),
    MORE(0x0001),
    ERROR(0x0002),
    IN_XACT(0x0004),
    COUNT(0x0010),
    ATTN(0x0020),
    SERVER_ERROR(0x0100);

    private final int value;

    DoneStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static DoneStatus fromValue(int value) {
        for (DoneStatus s : values()) {
            if ((value & s.value) == s.value) {
                return s;
            }
        }
        return FINAL;
    }

    public boolean isSet(int combined) {
        return (combined & value) != 0;
    }

    @Override
    public String toString() {
        return name() + " (0x" + Integer.toHexString(value) + ")";
    }
}