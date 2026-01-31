package io.s2c.network.message.reader;

public enum Failure {
  // MessageTooLargeException is currently not injected as it is considered fatal and is handled
  // by throwing IllegalStateException -> no special behavior to test
  DROP, IO_EXCEPTION
}