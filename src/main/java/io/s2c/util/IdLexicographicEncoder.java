package io.s2c.util;

public class IdLexicographicEncoder {
  private static final int WIDTH = 19;

  public String encode(long id) {
    if (id < 0) throw new IllegalArgumentException("Cannot encode negative numbers");
    return String.format("%0" + WIDTH + "d", id);
  }
}
