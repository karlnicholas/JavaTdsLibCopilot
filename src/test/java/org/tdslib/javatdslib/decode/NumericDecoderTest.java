package org.tdslib.javatdslib.decode;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.tdslib.javatdslib.codec.NumericDecoder;
import org.tdslib.javatdslib.protocol.TdsType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

class NumericDecoderTest {

  private final NumericDecoder decoder = new NumericDecoder();

  @Test
  void testDecimalSignBitEdgeCases() {
    // TDS Decimal Format: [0] = Sign (1 pos, 0 neg), [1..N] = Little-Endian Magnitude
    // Representing -123.45 with scale 2
    // Magnitude 12345 = 0x3039. In TDS bytes (LE): [0x39, 0x30]
    byte[] negativeBytes = new byte[] { 0, 0x39, 0x30 };
    byte[] positiveBytes = new byte[] { 1, 0x39, 0x30 };

    BigDecimal negResult = decoder.decode(negativeBytes, TdsType.DECIMAL, BigDecimal.class, 2, StandardCharsets.UTF_8);
    BigDecimal posResult = decoder.decode(positiveBytes, TdsType.DECIMAL, BigDecimal.class, 2, StandardCharsets.UTF_8);

    assertEquals(new BigDecimal("-123.45"), negResult, "Negative sign bit (0) failed");
    assertEquals(new BigDecimal("123.45"), posResult, "Positive sign bit (1) failed");
  }

  @Test
  void testUnsignedTinyIntFix() {
    // SQL Server TINYINT 255 is 0xFF.
    // Java bytes are signed, so (byte)0xFF is -1.
    // Our fix: (data[0] & 0xFF)
    byte[] tinyIntBytes = new byte[] { (byte) 0xFF };

    Integer result = decoder.decode(tinyIntBytes, TdsType.INT1, Integer.class, 0, StandardCharsets.UTF_8);

    assertEquals(255, result, "TINYINT sign extension fix failed; should be 255, not -1");
  }

  @Test
  void testMoneyLargeValue() {
    // MS-TDS MONEY quirk: It is NOT a standard 64-bit little-endian long.
    // It is sent as two 32-bit integers: HIGH 32-bits first, then LOW 32-bits.
    ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);

    long maxValue = Long.MAX_VALUE; // 9223372036854775807
    int high = (int) (maxValue >> 32);
    int low = (int) maxValue;

    bb.putInt(high);
    bb.putInt(low);

    BigDecimal result = decoder.decode(bb.array(), TdsType.MONEY, BigDecimal.class, 4, StandardCharsets.UTF_8);

    assertEquals(new BigDecimal("922337203685477.5807"), result);
  }
}