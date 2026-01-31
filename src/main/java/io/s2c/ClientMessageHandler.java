package io.s2c;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.s2c.concurrency.GuardedValue;
import io.s2c.error.ApplicationException;
import io.s2c.error.ApplicationResultUnavailableException;
import io.s2c.error.CommitException;
import io.s2c.error.ConcurrentStateModificationException;
import io.s2c.error.RequestOutOfSequenceException;
import io.s2c.error.S2CStoppedException;
import io.s2c.error.StateRequestException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.ApplicationError;
import io.s2c.model.messages.ApplicationResult;
import io.s2c.model.messages.ApplicationResultUnavailableError;
import io.s2c.model.messages.Follow;
import io.s2c.model.messages.FollowResponse;
import io.s2c.model.messages.InternalError;
import io.s2c.model.messages.LeaderStartingError;
import io.s2c.model.messages.NotFollowerError;
import io.s2c.model.messages.NotLeaderError;
import io.s2c.model.messages.RequestOutOfSequenceError;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.StateRequest;
import io.s2c.model.messages.StateRequestResponse;
import io.s2c.model.messages.SynchronizeRequest;
import io.s2c.model.messages.SynchronizeResponse;
import io.s2c.model.state.FollowerInfo;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.NodeIdentity;
import io.s2c.util.ConcurrentStateModificationExceptionHandler;

public class ClientMessageHandler {

  private final Logger logger = LoggerFactory.getLogger(ClientMessageHandler.class);
  private final StructuredLogger log;

  private final LeaderStateManager leaderStateManager;
  private final GuardedValue<Boolean> guardedLeaderStarting;
  private final Consumer<Heartbeat> heartbeatHandler;
  private final SynchronizeManager synchronizeManager;
  private final StateRequestHandler stateRequestHandler;
  private final AsyncBatchApplier asyncBatchApplier;
  private final ConcurrentStateModificationExceptionHandler concurrentStateModificationExceptionHandler;
  private final Supplier<Long> applyIndexSupplier;
  private final Runnable tooFarBehindHandler;
  private final AtomicInteger concurrentFollowHandling = new AtomicInteger();
  private final AtomicInteger concurrentSynchronizeHandling = new AtomicInteger();
  private final AtomicInteger concurrentStateReqHandling = new AtomicInteger();
  private final AtomicLong lastAcceptedCommitIndexForSynchronize = new AtomicLong();
  private final Function<NodeIdentity, Optional<Long>> clientSequenceNumberProvider;
  
  private Counter succeededStateRequests;
  private Counter failedStateRequests;
  private Counter rejectedStateRequestsNotFollower;
  private Counter rejectedStateRequestsNotLeader;
  private Counter rejectedStateRequestsLeaderStarting;
  private Counter rejectedStateRequestsConcurrentModification;

  private Counter succeededSynchRequests;
  private Counter rejectedSynchRequestsIndexOutOfOrder;
  private Counter rejectedSynchRequestsQueueFull;

  private Counter succeededFollowRequests;
  private Counter duplicateFollowRequests;
  private Counter rejectedFollowRequests;

  private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
  private volatile boolean leader = false;

  public ClientMessageHandler(ContextProvider contextProvider,
      LeaderStateManager leaderStateManager,
      Consumer<Heartbeat> heartbeatHandler,
      GuardedValue<Boolean> guardedLeaderStarting,
      SynchronizeManager synchronizeManager,
      StateRequestHandler stateRequestHandler,
      AsyncBatchApplier asyncBatchApplier,
      Supplier<Long> applyIndexSupplier,
      Runnable tooFarBehindHandler,
      ConcurrentStateModificationExceptionHandler concurrentStateModificationExceptionHandler,
      Function<NodeIdentity, Optional<Long>> clientSequenceNumberProvider,
      MeterRegistry meterRegistry) {
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.leaderStateManager = leaderStateManager;
    this.heartbeatHandler = heartbeatHandler;
    this.guardedLeaderStarting = guardedLeaderStarting;
    this.synchronizeManager = synchronizeManager;
    this.stateRequestHandler = stateRequestHandler;
    this.asyncBatchApplier = asyncBatchApplier;
    this.applyIndexSupplier = applyIndexSupplier;
    this.tooFarBehindHandler = tooFarBehindHandler;
    this.concurrentStateModificationExceptionHandler = concurrentStateModificationExceptionHandler;
    this.clientSequenceNumberProvider = clientSequenceNumberProvider;
    initMetrics(contextProvider, meterRegistry);

  }

  public S2CMessage handleFollow(String correlationId, Follow follow) {
    S2CMessage.Builder builder = S2CMessage.newBuilder().setCorrelationId(correlationId);
    concurrentFollowHandling.incrementAndGet();
    try {
      FollowerInfo followerInfo = FollowerInfo.newBuilder()
          .setNodeIdentity(follow.getNodeIdentity())
          .setApplyIndex(follow.getApplyIndex())
          .build();
      LeaderState leaderState = leaderStateManager.getLeaderState();
      if (!leaderStateManager.isLeader(leaderState)) {
        rejectedFollowRequests.increment();
        return builder.setNotLeaderError(NotLeaderError.getDefaultInstance()).build();
      }
      var logBuilder = log.debug()
          .addKeyValue("correlationId", correlationId)
          .addKeyValue("followerNodeIdentity", follow.getNodeIdentity());
      if (leaderStateManager.addNewFollower(followerInfo)) {
        synchronizeManager.syncFollower(followerInfo);
        logBuilder.log("Follower added");
        succeededFollowRequests.increment();
      } else {
        logBuilder.log("Follower already exists");
        duplicateFollowRequests.increment();
      }
      // We fallback to zero - zero is never a valid seq num as client must send nextSeqNum
      // (nextSeqNum=lastSeqNum + 1)
      Long seqNum = clientSequenceNumberProvider.apply(follow.getNodeIdentity()).orElse(0L);
      // Respond a follow response for either new or existing follower
      return builder.setFollowResponse(FollowResponse.newBuilder().setNodeSequenceNumber(seqNum))
          .build();
    }
    catch (InterruptedException | S2CStoppedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.debug().setCause(e).log("Error while handling follow request");
      return builder.setInternalError(InternalError.getDefaultInstance()).build();
    }
    finally {
      concurrentFollowHandling.decrementAndGet();
    }
  }

  public S2CMessage handleStateRequest(String correlationId, StateRequest stateRequest) {

    S2CMessage.Builder messageBuilder = S2CMessage.newBuilder().setCorrelationId(correlationId);
    var logBuilder = log.debug().addKeyValue("correlationId", correlationId);
    concurrentStateReqHandling.incrementAndGet();

    stateLock.readLock().lock();
    try {
      leader = true;
      LeaderState leaderState = leaderStateManager.getLeaderState();
      if (!leaderStateManager.isLeader(leaderState)) {
        logBuilder.log("Cannot handle because not leader");
        rejectedStateRequestsNotLeader.increment();
        messageBuilder.setNotLeaderError(NotLeaderError.getDefaultInstance());
      } else if (!leaderStateManager.isFollower(stateRequest.getSourceNode())
          && !stateRequest.getSourceNode().equals(leaderState.getNodeIdentity())) {
        logBuilder.log("Cannot handle because source node is not follower");
        rejectedStateRequestsNotFollower.increment();
        messageBuilder.setNotFollowerError(NotFollowerError.getDefaultInstance());
      } else {
        guardedLeaderStarting.read(leaderStarting -> {
          if (leaderStarting) {
            logBuilder.log("Cannot handle while leader starting");
            messageBuilder.setLeaderStartingError(LeaderStartingError.getDefaultInstance());
            rejectedStateRequestsLeaderStarting.increment();
            return;
          }
          // NodeStateManager is awaiting on RWSequencer to catch up
          // This can otherwise not be true, given the application awaits on this thread

          try {
            LeaderState leaderState2 = leaderStateManager.getLeaderState();
            if (applyIndexSupplier.get() < leaderState2.getCommitIndex()) {
              messageBuilder.setLeaderStartingError(LeaderStartingError.getDefaultInstance());
              rejectedStateRequestsLeaderStarting.increment();
              return;
            }
            handleAndSetResponse(correlationId, stateRequest, messageBuilder);
          }
          catch (S2CStoppedException | InterruptedException e) {
            log.error()
                .addKeyValue("correlationId", correlationId)
                .log("Error while handling request", e);
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
          }
          return;
        });
      }
    }
    catch (InterruptedException | S2CStoppedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.debug().setCause(e).log("Error while handling state request");
      messageBuilder.setInternalError(InternalError.getDefaultInstance());
    }
    finally {
      concurrentStateReqHandling.decrementAndGet();
      if (messageBuilder.hasNotLeaderError()) {
        leader = false;
      }
      stateLock.readLock().unlock();
    }

    return messageBuilder.build();
  }

  private void handleAndSetResponse(String correlationId, StateRequest stateRequest,
      S2CMessage.Builder messageBuilder) throws S2CStoppedException, InterruptedException {
    try {
      ByteString result = stateRequestHandler.handle(correlationId, stateRequest);
      messageBuilder.setStateRequestResponse(StateRequestResponse.newBuilder()
          .setApplicationResult(ApplicationResult.newBuilder().setBody(result))
          .build());
      succeededStateRequests.increment();
    }
    catch (StateRequestException e) {
      log.error()
          .addKeyValue("correlationId", correlationId)
          .log("Error while handling state request", e);
      switch (e) {
      case CommitException ex -> {
        if (ex instanceof ConcurrentStateModificationException ex1) {
          rejectedStateRequestsConcurrentModification.increment();
          concurrentStateModificationExceptionHandler.accept(ex1);
        } else {
          rejectedStateRequestsNotLeader.increment();
        }
        messageBuilder.setNotLeaderError(NotLeaderError.getDefaultInstance());
      }
      case ApplicationException ex -> {
        failedStateRequests.increment();
        messageBuilder.setApplicationError(
            ApplicationError.newBuilder().setErrorMsg(ex.getMessage()).build());
      }
      case ApplicationResultUnavailableException ex -> {
        messageBuilder.setStateRequestResponse(StateRequestResponse.newBuilder()
            .setApplicationResultUnavailableError(
                ApplicationResultUnavailableError.getDefaultInstance())
            .build());
      }
      }
    }
    catch (RequestOutOfSequenceException e) {
      if (stateRequest.getSequenceNumber() == 1) {
        log.info();
      }
      messageBuilder.setRequestOutOfSequenceError(
          RequestOutOfSequenceError.newBuilder().setNextSeqNum(e.nextSeqNum()));
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug().setCause(e).log("Interrupted");
      messageBuilder.setInternalError(InternalError.getDefaultInstance());
    }
  }

  public S2CMessage handleSynchronize(String correlationId, SynchronizeRequest synchronize)
      throws InterruptedException {

    concurrentSynchronizeHandling.incrementAndGet();

    heartbeatHandler.accept(Heartbeat.defaultInstance());

    boolean locked = stateLock.writeLock().tryLock();
    try {
      if (locked && !leader) {
        try {
          if (!leaderStateManager.isLeader(leaderStateManager.getLeaderState())) {
            if (synchronize.hasFollowerTooFarBehindError()) {
              tooFarBehindHandler.run();
            }
            if (!synchronize.getBatch().getLogEntriesList().isEmpty()) { // Just a heartbeat
                                                                         // otherwise
              if (synchronize.getCommitIndex() != lastAcceptedCommitIndexForSynchronize.get() + 1) {
                rejectedSynchRequestsIndexOutOfOrder.increment();
                log.debug()
                    .addKeyValue("correlationId", correlationId)
                    .addKeyValue("lastAcceptedCommitIndex", lastAcceptedCommitIndexForSynchronize.get())
                    .addKeyValue("requestCommitIndex", synchronize.getBatch().getCommitIndex())
                    .log("Synchronize request cannot enqueued as commit index is out of order");
              } else {
                long before = lastAcceptedCommitIndexForSynchronize.get();
                long lastAcceptedCommitIndex = asyncBatchApplier.apply(synchronize.getBatch());
                if (lastAcceptedCommitIndex > before) {
                  succeededSynchRequests.increment();
                  log.trace()
                      .addKeyValue("correlationId", correlationId)
                      .addKeyValue("applyIndex", applyIndexSupplier.get())
                      .log("Synchronize request enqueued for handling");
                  lastAcceptedCommitIndexForSynchronize.set(lastAcceptedCommitIndex);
                } else {
                  log.trace()
                      .addKeyValue("correlationId", correlationId)
                      .log("Couldn't enqueue synchronize request as queue is full.");
                  rejectedSynchRequestsQueueFull.increment();
                }
              }
            }

          }
        }
        catch (S2CStoppedException e) {
          log.debug().setCause(e).log("Error while handling synchronize request");
        }

      }
    }
    finally {
      if (locked) {
        stateLock.writeLock().unlock();
      }
    }

    var syncRes = SynchronizeResponse.newBuilder()
        .setApplyIndex(lastAcceptedCommitIndexForSynchronize.get())
        .build();
    concurrentSynchronizeHandling.decrementAndGet();
    return S2CMessage.newBuilder()
        .setCorrelationId(correlationId)
        .setSynchronizeResponse(syncRes)
        .build();
  }

  private void initMetrics(ContextProvider contextProvider, MeterRegistry meterRegistry) {

    Gauge.builder("concurrent.follow.handling", concurrentFollowHandling::get)
        .register(meterRegistry);

    // Should never be > 1
    Gauge.builder("concurrent.synchronize.handling", concurrentSynchronizeHandling::get)

        .register(meterRegistry);

    Gauge.builder("concurrent.state.request.handling", concurrentStateReqHandling::get)
        .register(meterRegistry);

    this.succeededStateRequests = Counter.builder("handled.state.requests")
        .tag("outcome", "success")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.failedStateRequests = Counter.builder("handled.state.requests")
        .tag("outcome", "application.error")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.rejectedStateRequestsNotFollower = Counter.builder("handled.state.requests")
        .tag("outcome", "not.follower")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.rejectedStateRequestsNotLeader = Counter.builder("handled.state.requests")
        .tag("outcome", "not.leader.error")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.rejectedStateRequestsLeaderStarting = Counter.builder("handled.state.requests")
        .tag("outcome", "leader.starting.error")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.rejectedStateRequestsConcurrentModification = Counter.builder("handled.state.requests")
        .tag("outcome", "concurrent.modification.error")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.succeededSynchRequests = Counter.builder("handled.synch.requests")
        .tag("outcome", "success")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.rejectedSynchRequestsIndexOutOfOrder = Counter.builder("handled.synch.requests")
        .tag("outcome", "index.out.of.order")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.rejectedSynchRequestsQueueFull = Counter.builder("handled.synch.requests")
        .tag("outcome", "queue.full")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.succeededFollowRequests = Counter.builder("handled.follow.requests")
        .tag("outcome", "success")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.rejectedFollowRequests = Counter.builder("handled.follow.requests")
        .tag("outcome", "not.leader.error")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    this.duplicateFollowRequests = Counter.builder("handled.follow.requests")
        .tag("outcome", "duplicate")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);
  }

}