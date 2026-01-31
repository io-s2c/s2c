package io.s2c.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IdLexicographicEncoderTest {

  private IdLexicographicEncoder encoder;

  @BeforeEach
  void setUp() {
    encoder = new IdLexicographicEncoder();
  }

  @Test
  void testEncodeZero() {
    String encoded = encoder.encode(0);
    assertEquals("0000000000000000000", encoded);
    assertEquals(19, encoded.length());
  }

  @Test
  void testEncodeOne() {
    String encoded = encoder.encode(1);
    assertEquals("0000000000000000001", encoded);
    assertEquals(19, encoded.length());
  }

  @Test
  void testEncodeSmallNumber() {
    String encoded = encoder.encode(123);
    assertEquals("0000000000000000123", encoded);
    assertEquals(19, encoded.length());
  }

  @Test
  void testEncodeLargeNumber() {
    String encoded = encoder.encode(1234567890123456789L);
    assertEquals("1234567890123456789", encoded);
    assertEquals(19, encoded.length());
  }

  @Test
  void testEncodeMaxLong() {
    String encoded = encoder.encode(Long.MAX_VALUE);
    assertEquals(String.valueOf(Long.MAX_VALUE), encoded);
    assertEquals(19, encoded.length());
  }

  @Test
  void testLexicographicOrdering() {
    String encoded1 = encoder.encode(1);
    String encoded2 = encoder.encode(2);
    String encoded10 = encoder.encode(10);
    String encoded100 = encoder.encode(100);
    
    assertTrue(encoded1.compareTo(encoded2) < 0);
    assertTrue(encoded2.compareTo(encoded10) < 0);
    assertTrue(encoded10.compareTo(encoded100) < 0);
  }

  @Test
  void testConsistentWidth() {
    for (long i = 0; i < 1000; i++) {
      String encoded = encoder.encode(i);
      assertEquals(19, encoded.length());
    }
  }

  @Test
  void testNegativeNumber() {
    assertThrows(IllegalArgumentException.class, () -> encoder.encode(-1));
    
  }
}

