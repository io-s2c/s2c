package io.s2c;

import java.util.function.Supplier;

import com.google.protobuf.ByteString;

import io.s2c.error.ApplicationException;
import io.s2c.error.S2CStoppedException;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.StateRequest;
import io.s2c.model.messages.StateRequest.StateRequestType;
import io.s2c.model.messages.StateRequestResponse;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.error.ClientStoppedException;

public abstract class S2CStateMachine {

  private String name;
  private String s2cGroupId;
  private NodeIdentity nodeIdentity;
  private Supplier<Long> sequenceNumberSupplier;
  private SubmitFunction submitFunction;

  final void init(String s2cGroupId, NodeIdentity nodeIdentity,
      Supplier<Long> sequenceNumberSupplier, String name, SubmitFunction submitFunction) {
    this.s2cGroupId = s2cGroupId;
    this.nodeIdentity = nodeIdentity;
    this.submitFunction = submitFunction;
    this.name = name;
    this.sequenceNumberSupplier = sequenceNumberSupplier;
  }

  protected final StateRequestResponse sendToLeader(ByteString body, StateRequestType requestType)
      throws ApplicationException {

    if (submitFunction == null || name == null) {
      throw new IllegalStateException(
          "ClientState machine is not initialized.. Did you use S2C::createAndRegisterStateMachine()?");
    }

    StateRequest.Builder stateRequestBuilder = StateRequest.newBuilder()
        .setBody(body)
        .setType(requestType)
        .setGroupId(s2cGroupId)
        .setSourceSm(name)
        .setSourceNode(nodeIdentity);

    if (requestType == StateRequestType.COMMAND) {
      stateRequestBuilder.setSequenceNumber(sequenceNumberSupplier.get());
    }

    try {
      S2CMessage res = submitFunction.submit(stateRequestBuilder.build());
      if (res.hasApplicationError()) {
        throw new ApplicationException("Error while applying state request");
      }
      return res.getStateRequestResponse();
    }
    catch (S2CStoppedException | InterruptedException | ClientStoppedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new ApplicationException("Error while applying state request", e);
    }

  }

  protected abstract ByteString snapshot();

  protected abstract void loadSnapshot(ByteString snapshot);

  final String name() {
    return this.name;
  }

  protected abstract ByteString handleRequest(ByteString request, StateRequestType requestType)
      throws ApplicationException;

  protected void handleInvalidCommandResponse() {
    throw new IllegalStateException(
        "StateRequestResponse for a COMMAND has neither ApplicationResult nor ApplicationResultUnavailableError");
  }

  protected void handleInvalidReadResponse() {
    throw new IllegalStateException(
        "StateRequestResponse for a READ request must have ApplicationResult");
  }
}