package io.s2c;

import java.util.function.Supplier;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.s2c.error.OperationNotPermittedException;
import io.s2c.error.S2CNodeStoppedException;
import io.s2c.model.messages.InternalStateRequest;
import io.s2c.model.messages.S2CGroupStatusRequest;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.StateRequest;
import io.s2c.model.messages.StateRequest.StateRequestType;
import io.s2c.model.messages.StateRequestResponse;
import io.s2c.model.state.NodeIdentity;
import io.s2c.model.state.S2CGroupStatus;

public abstract class S2CStateMachine {

  private String name;
  private String s2cGroupId;
  private NodeIdentity nodeIdentity;
  private Supplier<Long> sequenceNumberSupplier;
  private Supplier<Long> localApplyIndexSupplier;
  private SubmitFunction submitFunction;
  
  final void init(String s2cGroupId, NodeIdentity nodeIdentity,
      Supplier<Long> sequenceNumberSupplier, Supplier<Long> localApplyIndexSupplier, String name, SubmitFunction submitFunction) {
    this.s2cGroupId = s2cGroupId;
    this.nodeIdentity = nodeIdentity;
    this.sequenceNumberSupplier = sequenceNumberSupplier;
    this.localApplyIndexSupplier = localApplyIndexSupplier;
    this.submitFunction = submitFunction;
    this.name = name;
  }

  protected final NodeIdentity nodeIdentity() {
    return this.nodeIdentity;
  }

  void consumeRoleTransition(boolean isLeader) {
    handleRoleTransition(isLeader);
  }
  
  protected void handleRoleTransition(boolean isLeader) {
    
  }

  protected final S2CGroupStatus s2cGroupStatus()
      throws S2CNodeStoppedException, InterruptedException {

    S2CGroupStatusRequest request = S2CGroupStatusRequest.getDefaultInstance();
    InternalStateRequest internalStateRequest = InternalStateRequest.newBuilder()
        .setGroupStatusRequest(request)
        .build();
    try {
      ByteString result = sendInternal(internalStateRequest, StateRequestType.READ);
      return S2CGroupStatus.parseFrom(result);
    }
    catch (InvalidProtocolBufferException e) {
      throw fatal(e);
    }
  }

  private final S2CMessage sendToLeader(ByteString body, StateRequestType requestType,
      boolean internal, boolean leaderCommand)
      throws S2CNodeStoppedException, InterruptedException {
    if (submitFunction == null || name == null) {
      throw new IllegalStateException(
          "ClientState machine is not initialized.. Did you use S2CNode::createAndRegisterStateMachine()?");
    }

    StateRequest.Builder stateRequestBuilder = StateRequest.newBuilder()
        .setBody(body)
        .setType(requestType)
        .setGroupId(s2cGroupId)
        .setSourceSm(name)
        .setSourceNode(nodeIdentity)
        .setInternal(internal)
        .setLeaderCommand(leaderCommand);

    if (requestType == StateRequestType.COMMAND) {
      stateRequestBuilder.setSequenceNumber(sequenceNumberSupplier.get());
    }

    S2CMessage res;
    res = submitFunction.submit(stateRequestBuilder.build());
    if (res.hasNotLeaderError() && !leaderCommand) {
      // non leader command should be retried and eventually redirected the leader.
      throw new IllegalStateException("Unexpected response");
    }
    return res;
  }

  /**
   * A special convenience method that enables the state machine implementation to issue commands
   * that need to be committed only and only if the current node is the leader, otherwise
   * OperationNotPermittedException is thrown, which means either:
   * <ul>
   * <li>The current node is not leader.</li>
   * <li>Or the current node was leader but lost leadership while trying to commit the command.</li>
   * </ul>
   * In other words, OperationNotPermittedException means the node is not leader <b>at the moment it
   * was thrown.</b> It also guarantees that the command was neither committed nor executed.
   * Otherwise, the command was committed and applied by the leader. Like all commands, once the
   * command is committed, it will be applied by all nodes.
   */
  protected final StateRequestResponse executeLeaderCommand(ByteString command)
      throws S2CNodeStoppedException, InterruptedException,
      OperationNotPermittedException {
    S2CMessage res = sendToLeader(command, StateRequestType.COMMAND, false, true);
    if (res.hasNotLeaderError()) {
      throw new OperationNotPermittedException();
    }
    return res.getStateRequestResponse();
  }

  protected final StateRequestResponse sendToLeader(ByteString body, StateRequestType requestType)
      throws S2CNodeStoppedException, InterruptedException {
    return sendToLeader(body, requestType, false, false).getStateRequestResponse();

  }

  protected abstract ByteString snapshot();

  protected abstract void loadSnapshot(ByteString snapshot);

  protected final String name() {
    return this.name;
  }

  protected abstract ByteString handleRequest(ByteString request, StateRequestType requestType);

  protected long localApplyIndex() {
    return localApplyIndexSupplier.get();
  }

  private IllegalStateException fatal(Throwable e) {
    throw new IllegalStateException("Unexpected result", e);
  }

  private ByteString sendInternal(InternalStateRequest internalStateRequest,
      StateRequestType stateRequestType) throws S2CNodeStoppedException, InterruptedException {

      StateRequestResponse response = sendToLeader(internalStateRequest.toByteString(),
          stateRequestType,
          true,
          false).getStateRequestResponse();
      return response.getApplicationResult()
          .getBody();
    
  }
}