package io.s2c;

import io.s2c.error.S2CStoppedException;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.StateRequest;
import io.s2c.network.error.ClientStoppedException;

@FunctionalInterface
public interface SubmitFunction {
  public S2CMessage submit(StateRequest stateRequest) throws S2CStoppedException, InterruptedException, ClientStoppedException;
}
