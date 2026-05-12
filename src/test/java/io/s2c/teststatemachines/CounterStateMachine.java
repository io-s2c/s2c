package io.s2c.teststatemachines;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

import io.s2c.S2CStateMachine;
import io.s2c.error.S2CNodeStoppedException;
import io.s2c.model.messages.StateRequest.StateRequestType;
import io.s2c.model.messages.StateRequestResponse;
import io.s2c.model.state.S2CGroupStatus;

public class CounterStateMachine extends S2CStateMachine {

  private final AtomicInteger counter = new AtomicInteger();

  private static final String INCREMENT = "INCREMENT";
  private static final String DECREMENT = "DECREMENT";
  private static final String GET = "GET";
  private static final ByteString BAD_REQUEST = ByteString.copyFromUtf8("BAD_REQUEST");

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
  protected ByteString handleRequest(ByteString request, StateRequestType requestType) {
    String str = request.toString(StandardCharsets.UTF_8);
    if (str.equals(INCREMENT)) {
      counter.incrementAndGet();
    } else if (str.equals(DECREMENT)) {
      counter.decrementAndGet();
    } else if (!str.equals(GET)) {
      return BAD_REQUEST;
    }
    ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
    buf.putInt(counter.get());
    return ByteString.copyFrom(buf.array());
  }

  public Integer increment() throws S2CNodeStoppedException, InterruptedException {
    StateRequestResponse response = sendToLeader(
        ByteString.copyFrom(INCREMENT, StandardCharsets.UTF_8),
        StateRequestType.COMMAND);
    return parseResponse(response);
  }

  public Integer decrement() throws S2CNodeStoppedException, InterruptedException {
    StateRequestResponse response = sendToLeader(
        ByteString.copyFrom(DECREMENT, StandardCharsets.UTF_8),
        StateRequestType.COMMAND);
    return parseResponse(response);
  }

  private Integer parseResponse(StateRequestResponse response)
      throws S2CNodeStoppedException, InterruptedException {
    if (response.hasApplicationResultUnavailableError()) {
      return get();
    } else {
      return ByteBuffer.wrap(response.getApplicationResult()
          .getBody()
          .toByteArray())
          .getInt();
    }
  }

  public Integer get() throws S2CNodeStoppedException, InterruptedException {
    StateRequestResponse response = sendToLeader(ByteString.copyFrom(GET, StandardCharsets.UTF_8),
        StateRequestType.READ);
    return ByteBuffer.wrap(response.getApplicationResult()
        .getBody()
        .toByteArray())
        .getInt();
  }

  public S2CGroupStatus getS2SGroupStatus() throws S2CNodeStoppedException, InterruptedException {
    return s2cGroupStatus();
  }

}
