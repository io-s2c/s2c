package io.s2c;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.s2c.concurrency.Task;
import io.s2c.configs.S2COptions;
import io.s2c.error.ConnectTimeoutException;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.FollowerTooFarBehindError;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.SynchronizeRequest;
import io.s2c.model.state.FollowerInfo;
import io.s2c.network.S2CClient;
import io.s2c.network.error.ClientConnectingException;
import io.s2c.network.error.ClientNotConnectedException;
import io.s2c.network.error.ClientStoppedException;
import io.s2c.network.error.FollowerTooFarBehindException;
import io.s2c.network.error.UnknownHostException;
import io.s2c.util.BackoffCounter;
import io.s2c.util.KeysResolver;

public class FollowerSynchronizer implements AutoCloseable, Task {

  private static final Long POISON_PILL = Long.valueOf(Long.MIN_VALUE);

  private final Logger logger = LoggerFactory.getLogger(FollowerSynchronizer.class);

  private final StructuredLogger log;

  private final S2CClient s2cClient;
  private final FollowerInfo followerInfo;
  private final Consumer<FollowerInfo> freeUp;
  private final S2CLog s2cLog;
  private final S2COptions s2cOptions;
  private final LeaderStateManager leaderStateManager;
  private final BlockingQueue<Long> requests = new LinkedBlockingQueue<>();
  private final KeysResolver keysResolver;
  private final ContextProvider contextProvider;
  private final long heartbeatDelay;
  private volatile boolean started = false;
  private volatile boolean followerBusy = false;
  private long followerApplyIndex = 0;

  private Timer syncRequestsTimer;
  private Counter failedReqsCounter;

  public FollowerSynchronizer(FollowerInfo followerInfo,
      S2CLog s2cLog,
      Consumer<FollowerInfo> freeUp,
      S2COptions s2cOptions,
      ContextProvider contextProvider,
      Supplier<S2CClient> s2cClientFactory,
      LeaderStateManager leaderStateManager,
      MeterRegistry meterRegistry) {

    this.followerInfo = followerInfo;
    this.s2cLog = s2cLog;
    this.freeUp = freeUp;
    this.s2cOptions = s2cOptions;
    this.s2cClient = s2cClientFactory.get();
    this.leaderStateManager = leaderStateManager;
    this.contextProvider = contextProvider;
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.keysResolver = new KeysResolver(contextProvider.s2cGroupId());
    this.heartbeatDelay = s2cOptions.leaderHeartbeatTimeoutMs() / 2;
    initMetrics(followerInfo, contextProvider, meterRegistry);

  }

  @Override
  public void run() {
    try {
      connectToFollower();
      if (!s2cClient.isReady()) {
        close();
        return;
      }
      log.debug().log("Connected to follower");
      startSyncLoop();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug().setCause(e).log("Interrupted");
      closeQuietly();
    }
  }

  private void connectToFollower() throws InterruptedException {
    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cOptions.s2cRetryOptions())
        .build();
    while (backoffCounter.canAttempt() && started) {
      try {
        s2cClient.connect(followerInfo.getNodeIdentity());
        break;
      }
      catch (ConnectTimeoutException | ClientNotConnectedException | IOException
          | UnknownHostException e) {
        log.debug().setCause(e).log("Error while connecting to follower");
        if (backoffCounter.canAttempt() && started) {
          backoffCounter.enrich(log.debug()).log("Retrying");
          backoffCounter.awaitNextAttempt();
        } else {
          backoffCounter.enrich(log.debug()).log("Cannot connect to follower after retries.");
        }
      }
    }
  }

  private void startSyncLoop() {
    BackoffCounter timeoutBackoffCounter = BackoffCounter
        .withRetryOptions(s2cOptions.s2cRetryOptions())
        .build();
    BackoffCounter followerBusyBackoffCounter = BackoffCounter
        .withRetryOptions(s2cOptions.s2cRetryOptions())
        .unlimited()
        .build();
    boolean timedout = false;
    Long commitIndex = null;

    while (started) {
      try {
        if (!leaderStateManager.isLeader(leaderStateManager.getLeaderState())) {
          closeQuietly();
          return;
        }

        if (timedout) {
          if (!timeoutBackoffCounter.canAttempt()) {
            closeQuietly();
            break;
          }
          timeoutBackoffCounter.awaitNextAttempt();
        }
        if (followerBusy) {
          followerBusyBackoffCounter.awaitNextAttempt();
        } else {
          followerBusyBackoffCounter.reset();
        }

        if (commitIndex != null) {
          doSyncToCommitIndex(commitIndex);
          if (followerBusy)
            continue;
        }

        commitIndex = requests.poll(heartbeatDelay, TimeUnit.MILLISECONDS);
        if (commitIndex == null) {
          send(SynchronizeRequest.getDefaultInstance()); // Heartbeat
          continue;
        }

        if (commitIndex == POISON_PILL) {
          return;
        } else {
          doSyncToCommitIndex(commitIndex);
          if (followerBusy) {
            continue;
          }
          commitIndex = null;
        }
      }
      catch (ClientConnectingException | ClientNotConnectedException ignore) {
        // No concurrency
      }
      catch (InterruptedException | S2CStoppedException e) {
        log.debug().setCause(e).log("Error in synchronize loop");
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        return;
      }

      catch (ClientStoppedException e) {
        closeQuietly();
        break;
      }
      catch (TimeoutException e) {
        timedout = true;
        continue;
      }
      catch (IOException e) {
        log.debug().setCause(e).log("Error while sending");
        if (e instanceof InvalidProtocolBufferException ex) {
          throw new IllegalStateException("Invalid message", ex);
        } else {
          reconnectToFollower();
        }
      }
      timedout = false;
      timeoutBackoffCounter.reset();
    }

  }

  private void reconnectToFollower() {
    try {
      log.debug().log("Reconnecting to follower");
      s2cClient.disconnect();
      connectToFollower();
      if (!s2cClient.isReady()) {
        log.debug().log("Reconnection failed");
        closeQuietly();
      } else {
        log.debug().log("Reconnected to follower");
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug().setCause(e).log("Interrupted");
      closeQuietly();
    }

  }

  private void doSyncToCommitIndex(Long commitIndex)
      throws InterruptedException, IOException, ClientStoppedException, TimeoutException,
      ClientNotConnectedException, ClientConnectingException {

    while (commitIndex != followerApplyIndex) {
      long batchIndex = followerApplyIndex + 1;
      S2CMessage res = null;
      Timer.Sample sample = Timer.start();
      try {
        SynchronizeRequest req = newSyncReq(batchIndex);
        long before = followerApplyIndex;
        res = send(req);
        followerApplyIndex = res.getSynchronizeResponse().getApplyIndex();
        if (followerApplyIndex == before) { // Follower's queue is full
          followerBusy = true;
          break;
        } else {
          followerBusy = false;
        }
      }
      catch (FollowerTooFarBehindException e) {
        log.debug().log(e.getMessage());
        log.debug()
            .addKeyValue("followerNodeIdentity", followerInfo::getNodeIdentity)
            .log("Removing follower");
        // Cause follower to re catch up
        SynchronizeRequest errReq = SynchronizeRequest.newBuilder()
            .setFollowerTooFarBehindError(FollowerTooFarBehindError.getDefaultInstance())
            .build();
        send(errReq);
        closeQuietly();
      }
      finally {
        if (res != null) {
          sample.stop(syncRequestsTimer);
        } else {
          failedReqsCounter.increment();
        }
      }
    }
  }

  private SynchronizeRequest newSyncReq(long batchIndex) throws FollowerTooFarBehindException {

    var batch = s2cLog.getBatchAt(keysResolver.logEntryKey(batchIndex));
    if (batch.isEmpty()) {
      // Entry must have been truncated
      throw new FollowerTooFarBehindException(
          "Entry at index %d might have been truncated and cannot synchronize follower anymore"
              .formatted(batchIndex));
    }
    return SynchronizeRequest.newBuilder().setBatch(batch.get()).setCommitIndex(batchIndex).build();
  }

  private S2CMessage send(SynchronizeRequest syncReq)
      throws InterruptedException, IOException, ClientConnectingException, ClientStoppedException,
      TimeoutException, ClientNotConnectedException {

    String correlationId = UUID.randomUUID().toString();

    S2CMessage s2cMessage = S2CMessage.newBuilder()
        .setCorrelationId(correlationId)
        .setSynchronizeRequest(syncReq)
        .build();

    return s2cClient.send(s2cMessage, s2cOptions.requestTimeoutMs());

  }

  private void closeQuietly() {
    try {
      close();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error().log("Interrupted");
    }
  }

  @Override
  public void close() throws InterruptedException {
    started = false;
    s2cClient.close();
    freeUp.accept(followerInfo);
    requests.put(POISON_PILL);
  }

  public void syncToCommitIndex(Long commitIndex) {
    requests.offer(commitIndex);
  }

  private void initMetrics(FollowerInfo followerInfo, ContextProvider contextProvider,
      MeterRegistry meterRegistry) {
    syncRequestsTimer = Timer.builder("sync.requests.latency")
        .tag("followerId", followerInfo.getNodeIdentity().getId())
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    failedReqsCounter = Counter.builder("sync.requests.failed")
        .tag("followerId", followerInfo.getNodeIdentity().getId())
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    Gauge.builder("sync.queue", requests::size)
        .tag("followerId", followerInfo.getNodeIdentity().getId())
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);
  }

  @Override
  public void init() {
    started = true;
  }

}