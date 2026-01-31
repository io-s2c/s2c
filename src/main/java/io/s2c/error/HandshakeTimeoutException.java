package io.s2c.error;

public class HandshakeTimeoutException extends Exception {
  public HandshakeTimeoutException(Throwable cause) {
    super(cause);
  }
}
