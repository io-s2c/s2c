package io.s2c;

public record Heartbeat() {
  private static final Heartbeat defaultInstance = new Heartbeat();

  public static Heartbeat defaultInstance() { return defaultInstance; }

}
