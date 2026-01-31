package io.s2c.teststatemachines;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;

import io.s2c.S2CStateMachine;
import io.s2c.error.ApplicationException;
import io.s2c.model.messages.StateRequest.StateRequestType;
import io.s2c.model.messages.StateRequestResponse;

public class StringAppender extends S2CStateMachine {

  private final StringBuilder stringBuilder = new StringBuilder();
  
  @Override
  public ByteString snapshot() {
    return ByteString.copyFromUtf8(stringBuilder.toString());
  }

  @Override
  protected void loadSnapshot(ByteString snapsohot) {
    String value = new String(snapsohot.toByteArray(), StandardCharsets.UTF_8);
    stringBuilder.delete(0, stringBuilder.length());
    stringBuilder.append(value);

  }

  @Override
  protected ByteString handleRequest(ByteString request, StateRequestType requestType)
      throws ApplicationException {
    if (requestType == StateRequestType.COMMAND) {
      String str = new String (request.toByteArray(), StandardCharsets.UTF_8);
      stringBuilder.append(str);
      return ByteString.copyFromUtf8(stringBuilder.toString());
    } else {
      return ByteString.copyFromUtf8(stringBuilder.toString());
    }

  }

  public void append(String str) throws ApplicationException {
    var byteStr = ByteString.copyFrom(str.getBytes(StandardCharsets.UTF_8));
    sendToLeader(byteStr, StateRequestType.COMMAND);
  }

  public String value() throws ApplicationException {
    ByteString byteStr = ByteString.copyFrom("GET".getBytes(StandardCharsets.UTF_8));
    StateRequestResponse response = sendToLeader(byteStr, StateRequestType.READ);
    String result = null;
    if (response.hasApplicationResult()) {
      result = new String(response.getApplicationResult().getBody().toByteArray(), StandardCharsets.UTF_8);
    } else {
      handleInvalidReadResponse();
    }
    return result;
  }

}
