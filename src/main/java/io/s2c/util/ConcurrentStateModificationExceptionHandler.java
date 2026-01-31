package io.s2c.util;

import io.s2c.error.ConcurrentStateModificationException;
import io.s2c.error.S2CStoppedException;

@FunctionalInterface
public interface ConcurrentStateModificationExceptionHandler {
  void accept(ConcurrentStateModificationException e)
      throws S2CStoppedException, InterruptedException;
}
