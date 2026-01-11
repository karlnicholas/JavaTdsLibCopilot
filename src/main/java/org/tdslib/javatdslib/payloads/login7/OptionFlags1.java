package org.tdslib.javatdslib.payloads.login7;

/**
 * TDS Login7 OptionFlags1.
 * Default constructor configures flags via setters, resulting in 0xE0.
 */
public final class OptionFlags1 {

    // === Nested enums (moved from top-level to satisfy OneTopLevelClass rule) ===

    private static enum OptionEndian {
        LittleEndian, BigEndian
    }

    private static enum OptionCharset {
        Ascii, Ebcdic
    }

    private static enum OptionFloat {
        IEEE, VAX, ND5000
    }

    private static enum OptionBcpDumpload {
        On, Off
    }

    private static enum OptionUseDb {
        On, Off
    }

    private static enum OptionInitDb {
        Warn, Fatal
    }

    private static enum OptionLangWarn {
        On, Off
    }

    private static final int OPTION_ENDIAN_BIT       = 0x01;
    private static final int OPTION_CHARSET_BIT      = 0x02;
    private static final int OPTION_FLOAT_VAX_BIT    = 0x04;
    private static final int OPTION_FLOAT_ND5000_BIT = 0x08;
    private static final int OPTION_BCP_DUMPLOAD_BIT = 0x10;
    private static final int OPTION_USE_DB_BIT       = 0x20;
    private static final int OPTION_INIT_DB_BIT      = 0x40;
    private static final int OPTION_LANG_WARN_BIT    = 0x80;

    private byte value;

    /**
     * Get raw flags byte.
     *
     * @return raw flags byte.
     */
    public byte getValue() {
        return value;
    }

    /**
     * Get endianness option.
     *
     * @return current endianness.
     */
    public OptionEndian getEndian() {
        if ((value & OPTION_ENDIAN_BIT) != 0) {
            return OptionEndian.BigEndian;
        }
        return OptionEndian.LittleEndian;
    }

    /**
     * Get charset option.
     *
     * @return current charset.
     */
    public OptionCharset getCharset() {
        if ((value & OPTION_CHARSET_BIT) != 0) {
            return OptionCharset.Ebcdic;
        }
        return OptionCharset.Ascii;
    }

    /**
     * Get floating format option.
     *
     * @return current float option.
     */
    public OptionFloat getFloat() {
        if ((value & OPTION_FLOAT_VAX_BIT) != 0) {
            return OptionFloat.VAX;
        }
        if ((value & OPTION_FLOAT_ND5000_BIT) != 0) {
            return OptionFloat.ND5000;
        }
        return OptionFloat.IEEE;
    }

    /**
     * Get BCP dumpload option.
     *
     * @return current BCP dumpload option.
     */
    public OptionBcpDumpload getBcpDumpload() {
        if ((value & OPTION_BCP_DUMPLOAD_BIT) != 0) {
            return OptionBcpDumpload.Off;
        }
        return OptionBcpDumpload.On;
    }

    /**
     * Get UseDb option.
     *
     * @return current UseDb option.
     */
    public OptionUseDb getUseDb() {
        if ((value & OPTION_USE_DB_BIT) != 0) {
            return OptionUseDb.Off;
        }
        return OptionUseDb.On;
    }

    /**
     * Get InitDb option.
     *
     * @return current InitDb option.
     */
    public OptionInitDb getInitDb() {
        if ((value & OPTION_INIT_DB_BIT) != 0) {
            return OptionInitDb.Fatal;
        }
        return OptionInitDb.Warn;
    }

    /**
     * Get language warning option.
     *
     * @return current LangWarn option.
     */
    public OptionLangWarn getLangWarn() {
        if ((value & OPTION_LANG_WARN_BIT) != 0) {
            return OptionLangWarn.On;
        }
        return OptionLangWarn.Off;
    }

    // ── Setters ─────────────────────────────────────────────────────────────

    /**
     * Set endianness option.
     *
     * @param v option to set.
     */
    public void setEndian(final OptionEndian v) {
        if (v == OptionEndian.LittleEndian) {
            value &= ~OPTION_ENDIAN_BIT;
        } else {
            value |= OPTION_ENDIAN_BIT;
        }
    }

    /**
     * Set charset option.
     *
     * @param v option to set.
     */
    public void setCharset(final OptionCharset v) {
        if (v == OptionCharset.Ascii) {
            value &= ~OPTION_CHARSET_BIT;
        } else {
            value |= OPTION_CHARSET_BIT;
        }
    }

    /**
     * Set float option.
     *
     * @param v option to set.
     */
    public void setFloat(final OptionFloat v) {
        value &= ~(OPTION_FLOAT_VAX_BIT | OPTION_FLOAT_ND5000_BIT);
        if (v == OptionFloat.VAX) {
            value |= OPTION_FLOAT_VAX_BIT;
        }
        if (v == OptionFloat.ND5000) {
            value |= OPTION_FLOAT_ND5000_BIT;
        }
    }

    /**
     * Set BCP dumpload option.
     *
     * @param v option to set.
     */
    public void setBcpDumpload(final OptionBcpDumpload v) {
        if (v == OptionBcpDumpload.On) {
            value &= ~OPTION_BCP_DUMPLOAD_BIT;
        } else {
            value |= OPTION_BCP_DUMPLOAD_BIT;
        }
    }

    /**
     * Set UseDb option.
     *
     * @param v option to set.
     */
    public void setUseDb(final OptionUseDb v) {
        if (v == OptionUseDb.On) {
            value &= ~OPTION_USE_DB_BIT;
        } else {
            value |= OPTION_USE_DB_BIT;
        }
    }

    /**
     * Set InitDb option.
     *
     * @param v option to set.
     */
    public void setInitDb(final OptionInitDb v) {
        if (v == OptionInitDb.Warn) {
            value &= ~OPTION_INIT_DB_BIT;
        } else {
            value |= OPTION_INIT_DB_BIT;
        }
    }

    /**
     * Set LangWarn option.
     *
     * @param v option to set.
     */
    public void setLangWarn(final OptionLangWarn v) {
        if (v == OptionLangWarn.Off) {
            value &= ~OPTION_LANG_WARN_BIT;
        } else {
            value |= OPTION_LANG_WARN_BIT;
        }
    }

    // ── Constructors ────────────────────────────────────────────────────────

    /**
     * Default constructor - sets values using setters.
     * Results in value = 0xE0 (binary: 11100000).
     */
    public OptionFlags1() {
        // Start from clean state (all bits 0).
        this.value = 0;

        // Apply the desired defaults (these match the original 0xE0 behavior).
        setEndian(OptionEndian.LittleEndian);      // bit 0 = 0
        setCharset(OptionCharset.Ascii);           // bit 1 = 0
        setFloat(OptionFloat.IEEE);                // bits 2+3 = 00
        setBcpDumpload(OptionBcpDumpload.On);      // bit 4 = 0
        setUseDb(OptionUseDb.Off);                 // bit 5 = 1
        setInitDb(OptionInitDb.Fatal);             // bit 6 = 1
        setLangWarn(OptionLangWarn.On);            // bit 7 = 1 → highest bit set
    }

    /**
     * Construct from raw byte.
     *
     * @param value raw flags byte.
     */
    public OptionFlags1(final byte value) {
        this.value = value;
    }

    // ── Utilities ───────────────────────────────────────────────────────────

    /**
     * Convert flags to a byte.
     *
     * @return flags as byte.
     */
    public byte toByte() {
        return value;
    }

    /**
     * Construct OptionFlags1 from a byte.
     *
     * @param value raw flags byte.
     * @return new OptionFlags1 instance.
     */
    public static OptionFlags1 fromByte(final byte value) {
        return new OptionFlags1(value);
    }

    @Override
    public String toString() {
        String bin = String.format("%8s", Integer.toBinaryString(Byte.toUnsignedInt(value)))
                .replace(' ', '0');

        String header = String.format("OptionFlags1[0x%02X | 0b%s]", Byte.toUnsignedInt(value), bin);

        String fmtPart1 = " (Endian=%s, Charset=%s, Float=%s, Bcp=%s, ";
        String fmtPart2 = "UseDb=%s, InitDb=%s, LangWarn=%s)";
        String details = String.format(
                fmtPart1 + fmtPart2,
                getEndian(),
                getCharset(),
                getFloat(),
                getBcpDumpload(),
                getUseDb(),
                getInitDb(),
                getLangWarn()
        );

        return header + details;
    }

    /**
     * Quick test main method.
     *
     * @param args unused.
     */
    public static void main(final String[] args) {
        OptionFlags1 flags = new OptionFlags1();
        System.out.println(flags); // Should print ... 0xE0 | 0b11100000 ...
    }
}
