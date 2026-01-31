package io.s2c;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Sample;
import io.s2c.LogReplayer.ReplayerBrokenException;
import io.s2c.concurrency.Awaiter;
import io.s2c.concurrency.GuardedValue;
import io.s2c.concurrency.Task;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.configs.S2COptions;
import io.s2c.error.ConnectTimeoutException;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.Follow;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.S2CClient;
import io.s2c.network.error.ClientConnectingException;
import io.s2c.network.error.ClientNotConnectedException;
import io.s2c.network.error.ClientStoppedException;
import io.s2c.network.error.UnknownHostException;
import io.s2c.util.BackoffCounter;

public class NodeStateManager implements Task {

  public enum NodeState {
    JOINED, JOINING
  }

  private final ContextProvider contextProvider;
  private Timer joinLatency;

  private final GuardedValue<Boolean> guardedLeaderStarting;
  private final LeaderStateManager leaderStateManager;
  private final Awaiter<LeaderState, S2CStoppedException> leaderStateAwaiter;
  private final StructuredLogger log;
  private final Logger logger = LoggerFactory.getLogger(NodeStateManager.class);
  private final NodeIdentity nodeIdentity;
  private final RSM rsm;
  private final BiConsumer<Long, Long> logCleaner;
  private final S2CClient s2cClient;
  private final S2COptions s2cOptions;

  private final ReentrantLock stateLock = new ReentrantLock();
  private final Condition stateCondition = stateLock.newCondition();
  private final TaskExecutor taskExecutor;

  private final GuardedValue<Boolean> tooFarBehindGuardedValue = new GuardedValue<>(false);

  private volatile boolean running = false;

  private volatile NodeState nodeState = NodeState.JOINING;

  private final AtomicLong nodeSequenceNumber = new AtomicLong(0L);

  public NodeStateManager(LeaderStateManager leaderStateManager,
      S2CClient s2cClient,
      ContextProvider contextProvider,
      NodeIdentity nodeIdentity,
      GuardedValue<Boolean> guardedLeaderStarting,
      RSM rsm,
      BiConsumer<Long, Long> logCleaner,
      S2COptions s2cOptions,
      MeterRegistry meterRegistry) {
    this.leaderStateManager = leaderStateManager;
    this.leaderStateAwaiter = leaderStateManager.getNewAwaiter();
    this.s2cClient = s2cClient;
    this.contextProvider = contextProvider;
    this.nodeIdentity = nodeIdentity;
    this.guardedLeaderStarting = guardedLeaderStarting;
    this.rsm = rsm;
    this.logCleaner = logCleaner;
    this.s2cOptions = s2cOptions;
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.taskExecutor = new TaskExecutor("node-state-manager", log.uncaughtExceptionLogger(),
        meterRegistry);
    joinLatency = Timer.builder("s2c.node.join.latency").register(meterRegistry);
  }

  public void awaitJoined() throws InterruptedException {
    stateLock.lock();
    try {
      while (!nodeState.equals(NodeState.JOINED) && running) {
        stateCondition.await();
      }
    }
    finally {
      stateLock.unlock();
    }
  }

  @Override
  public void close() throws InterruptedException {
    running = false;
    taskExecutor.close();
  }

  @Override
  public void run() {
    try {

      while (running) {
        leaderStateAwaiter.await(l -> true);
        log.info()
            .addKeyValue("s2cGroupId", contextProvider.s2cGroupId())
            .log("Joining the S2C group");
        join();
      }
    }
    catch (S2CStoppedException | InterruptedException | ReplayerBrokenException
        | ClientStoppedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.debug().setCause(e).log("Error in state loop");
      try {
        close();
      }
      catch (InterruptedException e1) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e1).log("Interrupted");
      }
    }
  }

  private String correlationId() {
    return UUID.randomUUID().toString();
  }

  private void handleLeader(LeaderState leaderState)
      throws InterruptedException, ReplayerBrokenException, S2CStoppedException {
    guardedLeaderStarting.write(v -> true);
    log.info().log("Leader starting.");

    // Update leaderState after lock is acquired as ClientMessageHandler might have
    // acquired lock
    // before and StateRequestHandler might have incremented commitIndex
    leaderState = leaderStateManager.getLeaderState();
    try {
      if (s2cClient.isReady()) {
        s2cClient.close();
      }
      long commitIndex = leaderState.getCommitIndex();

      var stateSnapshotOptional = rsm.catchUp(commitIndex);
      if (stateSnapshotOptional.isPresent()) {
        var stateSnapshot = stateSnapshotOptional.get();
        taskExecutor.start("gc-%d".formatted(stateSnapshot.getEndApplyIndex()),
            Task.of(() -> logCleaner.accept(stateSnapshot.getStartApplyIndex(),
                stateSnapshot.getEndApplyIndex())));
      }
      if (rsm.applyIndex() != commitIndex) {
        leaderStateManager.firstCommitAsLeader(true);
        log.info()
            .addKeyValue("applyIndex", rsm.applyIndex())
            .addKeyValue("commitIndex", commitIndex)
            .log("Possible diverge state: no entry found at commitIndex");
      } else {
        // Can commit at commitIndex+1 as there was an entry at
        // commitIndex
        leaderStateManager.firstCommitAsLeader(false);
      }
      log.info().log("Finished starting.");
    }
    finally {
      guardedLeaderStarting.write(v -> false);
    }
  }

  private void join() throws InterruptedException, S2CStoppedException, ClientStoppedException,
      ReplayerBrokenException {

    stateLock.lock();
    try {
      Sample sample = Timer.start();

      nodeState = NodeState.JOINING;

      BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cOptions.s2cRetryOptions())
          .unlimited()
          .build();

      AtomicReference<LeaderState> leaderState = new AtomicReference<>(
          leaderStateManager.getLeaderState());

      while (running) {

        boolean joined = leaderStateManager.isLeader(leaderState.get());

        if (joined) {
          handleLeader(leaderState.get());
          log.info().log("Joined as leader");

        }

        if (!joined) {
          log.info().log("Trying to join as a follower.");
          tooFarBehindGuardedValue.write(tooFarBehind -> {
            if (tooFarBehind) {
              log.info().log("Node is too far behind leader state. Catching up");
              try {
                try {
                  rsm.catchUp(leaderState.get().getCommitIndex());
                }
                catch (ReplayerBrokenException e) {
                  log.debug().setCause(e).log("Error while catching up");
                  close();
                }
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug().setCause(e).log("Interrupted");
              }

            }
            return false;
          });
          joined = joinAsFollower(leaderState.get());
          if (joined) {
            log.info().log("Joined as follower");
          }
        }

        if (joined) {
          nodeState = NodeState.JOINED;
          stateCondition.signalAll();
          sample.stop(joinLatency);
          return;
        }

        backoffCounter.enrich(log.debug()).log("Failed joining. Retrying");
        backoffCounter.awaitNextAttempt();
        leaderState.set(leaderStateManager.checkLeaderState(leaderState.get()));

      }

    }
    finally {
      stateLock.unlock();

    }
  }

  private boolean joinAsFollower(LeaderState leaderState)
      throws InterruptedException, S2CStoppedException, ClientStoppedException {

    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cOptions.s2cRetryOptions())
        .build();

    if (s2cClient.isReady()) {
      s2cClient.disconnect(); // Disconnect from last leader
    }

    while (backoffCounter.canAttempt()) {
      try {
        if (!s2cClient.isReady()) {
          s2cClient.connect(leaderState.getNodeIdentity());
          log.debug()
              .addKeyValue("leaderNodeIdentity", leaderState.getNodeIdentity())
              .log("Connected to leader.");
        }
        tryFollow();
        return true;
      }
      catch (UnknownHostException | ConnectTimeoutException | IOException
          | ClientNotConnectedException | ClientConnectingException | TimeoutException e) {
        log.debug().setCause(e).log("Error while attempting to connect to current leader.");
      }

      if (backoffCounter.canAttempt()) {
        backoffCounter.awaitNextAttempt();
        backoffCounter.enrich(log.debug()).log("Retrying");
        continue;
      }
      backoffCounter.enrich(log.debug())
          .log(
              "Max attempts reached for connecting to current leader.. Checking leader state then retrying.");
    }
    return false;
  }

  private S2CMessage newFollowMessage() {
    Follow follow = Follow.newBuilder()
        .setGroupId(contextProvider.s2cGroupId())
        .setNodeIdentity(nodeIdentity)
        .setApplyIndex(rsm.applyIndex())
        .build();
    return S2CMessage.newBuilder().setCorrelationId(correlationId()).setFollow(follow).build();
  }

  private S2CMessage sendFollow(S2CMessage message) throws ClientNotConnectedException,
      ClientStoppedException, InterruptedException, TimeoutException, IOException {

    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cOptions.s2cRetryOptions())
        .build();

    S2CMessage res = null;

    while (true) {
      try {
        res = s2cClient.send(message, s2cOptions.requestTimeoutMs());
        break;
      }
      catch (TimeoutException | IOException e) {
        log.debug().setCause(e).log("Error while sending follow request");
        if (!backoffCounter.canAttempt()) {
          throw e;
        }
      }
      backoffCounter.enrich(log.debug()).log("Retrying.");
      backoffCounter.awaitNextAttempt();
    }

    return res;
  }

  private void tryFollow() throws InterruptedException, S2CStoppedException, ClientStoppedException,
      ClientNotConnectedException, ClientConnectingException, TimeoutException, IOException {

    LeaderState leaderState = leaderStateManager.getLeaderState();

    S2CMessage followMessage = newFollowMessage();

    log.trace()
        .addKeyValue("correlationId", followMessage.getCorrelationId())
        .addKeyValue("applyIndex", rsm.applyIndex())
        .addKeyValue("leaderNodeIdentity", leaderState.getNodeIdentity())
        .log("Sending follow request");

    // LeaderState object might not
    // change between request and response as the node won't do any
    // operations before receiving follow response.
    S2CMessage res = sendFollow(followMessage);
    log.debug().addKeyValue("response", res::toString).log("Leader responded");
    if (res != null && res.hasFollowResponse()) {
      nodeSequenceNumber.set(res.getFollowResponse().getNodeSequenceNumber());
      log.debug().log("Node is follower.");
    } else {
      // This should never happen
      throw new IllegalStateException("Invalid response from leader");
    }
  }

  public long nextSequenceNumber() throws InterruptedException {
    // Always start with 1 if leader has seqNum 0 -> 0 is never valid seq num
    awaitJoined();
    return nodeSequenceNumber.incrementAndGet();
  }

  @Override
  public void init() {
    running = true;
  }

  public void notifyTooFarBehind() {
    tooFarBehindGuardedValue.tryWrite(v -> true);
  }

  public NodeState nodeState() {
    return nodeState;
  }

}
