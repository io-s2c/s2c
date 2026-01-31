package io.s2c.error;

public class MessageTooLargeException extends Exception {
  public MessageTooLargeException(String msg) {
    super(msg);
  }
}
