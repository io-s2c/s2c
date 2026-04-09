package io.s2c;

import com.google.protobuf.ByteString;

import io.s2c.error.S2CStoppedException;
import io.s2c.model.messages.InternalStateRequest;

@FunctionalInterface
public interface InternalStateRequestHandler {
  // Returns ByteString because executed locally on the leader and the result is packed and sent to the client
  public ByteString handle(InternalStateRequest internalStateRequest) throws InterruptedException, S2CStoppedException;

}
