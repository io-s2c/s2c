package io.s2c.error;

public abstract sealed class StateRequestHandlingException extends Exception permits CommitException, ApplicationException, ApplicationResultUnavailableException {

  private static final long serialVersionUID = -7954114691568924283L;

  public StateRequestHandlingException() {
    super();
  }

  public StateRequestHandlingException(String msg) {
    super(msg);
  }

  public StateRequestHandlingException(String msg, Throwable e) {
    super(msg, e);
  }
}
