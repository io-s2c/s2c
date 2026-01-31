package io.s2c.error;

public class ConnectTimeoutException extends Exception {
  public ConnectTimeoutException(Throwable cause) {
    super(cause);
  }
}
