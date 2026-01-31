package io.s2c.error;

public abstract sealed class StateRequestException extends Exception permits CommitException, ApplicationException, ApplicationResultUnavailableException {

  public StateRequestException() {
    super();
  }

  public StateRequestException(String msg) {
    super(msg);
  }

  public StateRequestException(String msg, Throwable e) {
    super(msg, e);
  }
}
