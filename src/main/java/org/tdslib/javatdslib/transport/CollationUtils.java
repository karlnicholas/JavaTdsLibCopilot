package org.tdslib.javatdslib.transport;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Optional;

/**
 * Helper to derive Java Charset from TDS collation bytes (ENVCHANGE type 7).
 */
public class CollationUtils {

  /**
   * Attempts to map TDS collation bytes to a Java Charset.
   * Returns Optional.empty() if unknown or Unicode-only.
   */
  public static Optional<Charset> getCharsetFromCollation(byte[] collationBytes) {
    if (collationBytes == null || collationBytes.length < 5) {
      return Optional.empty();
    }

    // Read collation ID as little-endian unsigned 32-bit int
    ByteBuffer bb = ByteBuffer.wrap(collationBytes, 0, 4)
        .order(ByteOrder.LITTLE_ENDIAN);
    long collationId = bb.getInt() & 0xFFFFFFFFL;

    // Extract Windows LCID (lower 20 bits)
    int lcid = (int) (collationId & 0x000FFFFF);

    // For most legacy non-Unicode collations, code page is tied to LCID
    int codePage = getCodePageFromLcid(lcid);

    return codePageToCharset(codePage);
  }

  /**
   * Very partial mapping of common Windows LCID → code page.
   * Real-world drivers (jTDS, Microsoft JDBC) use similar tables.
   * Extend as needed for your use cases.
   */
  private static int getCodePageFromLcid(int lcid) {
    // Common examples – add more from syscharsets or Windows NLS tables
    return switch (lcid) {
      case 1033,  // en-US, Latin1_General
           2057,  // en-GB
           3081   // en-AU
          -> 1252;   // Windows-1252 (Latin1)

      case 1031 -> 1252;   // de-DE
      case 1036 -> 1252;   // fr-FR
      case 1040 -> 1252;   // it-IT

      case 1049 -> 1251;   // ru-RU, Windows-1251
      case 1053 -> 1252;   // sv-SE

      // Japanese, Chinese, Korean – often use multi-byte code pages
      case 1041 -> 932;    // Shift_JIS (cp932)
      case 2052 -> 936;    // GB2312 / GBK (cp936)
      case 1042 -> 949;    // EUC-KR / cp949

      default -> -1;       // Unknown → fallback to UTF-8 or throw
    };
  }

  /**
   * Maps Windows code page number → Java Charset.
   * Returns Optional.empty() if no good match.
   */
  private static Optional<Charset> codePageToCharset(int codePage) {
    return switch (codePage) {
      case 1252 -> Optional.of(Charset.forName("windows-1252")); // Use actual Windows-1252
      case 1251 -> Optional.of(Charset.forName("windows-1251"));
      case 1250 -> Optional.of(Charset.forName("windows-1250"));
      case 932  -> Optional.of(Charset.forName("windows-31j")); // Shift_JIS variant
      case 936  -> Optional.of(Charset.forName("GBK"));
      case 949  -> Optional.of(Charset.forName("EUC-KR"));
      case -1   -> Optional.empty();
      default   -> Optional.of(Charset.forName("windows-" + codePage));
    };
  }
}