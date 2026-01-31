package io.s2c.teststatemachines;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

import io.s2c.S2CStateMachine;
import io.s2c.error.ApplicationException;
import io.s2c.model.messages.StateRequest.StateRequestType;
import io.s2c.model.messages.StateRequestResponse;

public class CounterStateMachine extends S2CStateMachine {

  private final AtomicInteger counter = new AtomicInteger();

  private static final String INCREMENT = "INCREMENT";
  private static final String DECREMENT = "DECREMENT";
  private static final String GET = "GET";

  @Override
  public ByteString snapshot() {
    ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
    buf.putInt(counter.get());
    buf.flip();
    return ByteString.copyFrom(buf);
  }

  @Override
  protected void loadSnapshot(ByteString snapshot) {
    ByteBuffer buf = snapshot.asReadOnlyByteBuffer();
    counter.set(buf.getInt());
  }

  @Override
  protected ByteString handleRequest(ByteString request, StateRequestType requestType)
      throws ApplicationException {
    String str = request.toString(StandardCharsets.UTF_8);
    if (str.equals(INCREMENT)) {
      counter.incrementAndGet();
    } else if (str.equals(DECREMENT)) {
      counter.decrementAndGet();
    } else if (!str.equals(GET)) {
      throw new ApplicationException("Unknown command %s".formatted(str));
    }
    ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
    buf.putInt(counter.get());
    return ByteString.copyFrom(buf.array());
  }

  public Integer increment() throws ApplicationException {
    StateRequestResponse response = sendToLeader(ByteString.copyFrom(INCREMENT, StandardCharsets.UTF_8),
        StateRequestType.COMMAND);
    int val = 0;
    if (response.hasApplicationResult()) {
      val = ByteBuffer.wrap(response.getApplicationResult().getBody().toByteArray()).getInt();
    } else if (response.hasApplicationResultUnavailableError()) {
      val = get();
    } else {
      handleInvalidCommandResponse();
    }
    return val;
  }



  public Integer decrement() throws ApplicationException {
    StateRequestResponse response = sendToLeader(ByteString.copyFrom(DECREMENT, StandardCharsets.UTF_8),
        StateRequestType.COMMAND);
    int val = 0;
    if (response.hasApplicationResult()) {
      val = ByteBuffer.wrap(response.getApplicationResult().getBody().toByteArray()).getInt();
    } else if (response.hasApplicationResultUnavailableError()) {
      val = get();
    } else {
      handleInvalidCommandResponse();
    }
    return val;
  }

  public Integer get() throws ApplicationException {
    StateRequestResponse response = sendToLeader(ByteString.copyFrom(GET, StandardCharsets.UTF_8),
        StateRequestType.READ);
    int val = 0;
    if (response.hasApplicationResult()) {
      val = ByteBuffer.wrap(response.getApplicationResult().getBody().toByteArray()).getInt();
    } else {
      handleInvalidReadResponse();
    }
    return val;
  }

}
