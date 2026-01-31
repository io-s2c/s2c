package io.s2c.error;

import java.util.Objects;

public final class ApplicationException extends StateRequestException {
  private static final long serialVersionUID = 6488140240123323978L;

  public ApplicationException(String msg) {
    super(msg);
    Objects.requireNonNull(msg);
  }

  public ApplicationException(String msg, Throwable e) {
    super(msg, e);
    Objects.requireNonNull(msg);
  }
}