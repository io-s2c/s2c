package io.s2c.error;

public class S2CInterruptedException extends RuntimeException {
  public S2CInterruptedException(Throwable cause) {
    super(cause);
  }
}
