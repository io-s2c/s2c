package io.s2c.error;

public class RetriesExhaustedException extends Exception {

  public RetriesExhaustedException(Throwable cause) {
    super(cause);
  }

}
