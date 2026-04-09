package io.s2c;

import io.s2c.error.S2CNodeStoppedException;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.StateRequest;

@FunctionalInterface
public interface SubmitFunction {
  public S2CMessage submit(StateRequest stateRequest) throws InterruptedException, S2CNodeStoppedException;
}