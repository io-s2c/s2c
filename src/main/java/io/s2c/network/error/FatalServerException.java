package io.s2c.network.error;

import java.io.IOException;

public class FatalServerException extends Exception {
  public FatalServerException(IOException cause) {
    super(cause);
  }

  public FatalServerException(InterruptedException cause) {
    super(cause);
  }
}
