package io.s2c;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.s2c.concurrency.Awaiter;
import io.s2c.configs.S2COptions;
import io.s2c.error.ConcurrentStateModificationException;
import io.s2c.error.ConnectTimeoutException;
import io.s2c.error.LowRankNodeException;
import io.s2c.error.ObjectCorruptedException;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.FollowerInfo;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.S2CClient;
import io.s2c.network.error.ClientNotConnectedException;
import io.s2c.network.error.UnknownHostException;
import io.s2c.s3.ObjectReader;
import io.s2c.s3.ObjectWriter;
import io.s2c.util.BackoffCounter;
import io.s2c.util.KeysResolver;

public class LeaderStateManager implements AutoCloseable {

  private Timer attemptLatency;

  private Timer catchUpLatency;

  private Timer checkLatency;

  private Timer updateLatency;

  private volatile LeaderState currentLeaderState = null;
  private final ReentrantReadWriteLock currentLeaderStateLock = new ReentrantReadWriteLock();
  private final AtomicBoolean firstCommitAsLeader = new AtomicBoolean(false);
  private final Map<NodeIdentity, FollowerInfo> followers = new ConcurrentHashMap<>();
  private final List<Awaiter<LeaderState, S2CStoppedException>> awaiters = Collections
      .synchronizedList(new ArrayList<>());
  private final KeysResolver keysResolver;

  private volatile String leaderETag;
  private final StructuredLogger log;
  private final Logger logger = LoggerFactory.getLogger(LeaderStateManager.class);
  private final NodeIdentity nodeIdentity;
  private final ObjectReader objectReader;
  private final ObjectWriter objectWriter;
  private final Supplier<Long> applyIndexSupplier;
  private final Supplier<S2CClient> s2cClientFactory;
  private final S2COptions s2cOptions;
  private volatile boolean stopped = false;

  public LeaderStateManager(Function<StructuredLogger, ObjectReader> objectReaderFactory,
      Function<StructuredLogger, ObjectWriter> objectWriterFactory,
      S2COptions s2cOptions,
      ContextProvider contextProvider,
      Supplier<S2CClient> s2cClientFactory,
      Supplier<Long> applyIndexSupplier,
      MeterRegistry meterRegistry) {
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.objectReader = objectReaderFactory.apply(log);
    this.objectWriter = objectWriterFactory.apply(log);
    this.keysResolver = new KeysResolver(contextProvider.s2cGroupId());
    this.nodeIdentity = contextProvider.nodeIdentity();
    this.s2cOptions = s2cOptions;
    this.s2cClientFactory = s2cClientFactory;
    this.applyIndexSupplier = applyIndexSupplier;
    initMetrics(contextProvider.s2cGroupId(), meterRegistry);

  }

  public Awaiter<LeaderState, S2CStoppedException> getNewAwaiter() {
    Awaiter<LeaderState, S2CStoppedException> awaiter = new Awaiter<>(S2CStoppedException::new);
    awaiters.add(awaiter);
    return awaiter;
  }

  public boolean addNewFollower(FollowerInfo followerInfo) {
    return followers.putIfAbsent(followerInfo.getNodeIdentity(), followerInfo) == null;
  }

  public LeaderState checkLeaderState(LeaderState oldLeaderState)
      throws InterruptedException, S2CStoppedException {
    String eTagSnapshot = leaderETag;
    // First epoch is 1
    long oldEpoch = oldLeaderState == null ? 0 : oldLeaderState.getEpoch();
    verifyNotStopped();
    Timer.Sample sample = Timer.start();

    currentLeaderStateLock.writeLock().lockInterruptibly();
    try {
      if (oldLeaderState != currentLeaderState) {
        return currentLeaderState();
      }
      // Note that we compare by reference, because if reference changed, we know we
      // have new etag.
      if (eTagSnapshot == leaderETag) {
        ensureValidLeaderState(eTagSnapshot);
        long currentEpoch = currentLeaderState.getEpoch();
        // No new leader
        if (currentEpoch > oldEpoch) {
          notifyLeaderChange(currentLeaderState);
        }
      }
      return currentLeaderState();
    }
    finally {
      sample.stop(checkLatency);
      currentLeaderStateLock.writeLock().unlock();
    }
  }

  private void notifyLeaderChange(LeaderState leaderState) {
    awaiters.forEach(s -> {
      try {
        s.accept(leaderState);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e).log("Interrupted");
        closeQuietly();
      }
    });
  }

  public void closeQuietly() {
    try {
      close();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug().setCause(e).log("Interrupted while closing");
    }
  }

  @Override
  public void close() throws InterruptedException {
    currentLeaderStateLock.writeLock().lockInterruptibly();
    stopped = true;

    try {
      awaiters.forEach(a -> {
        try {
          a.stop();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.debug().setCause(e).log("Interrupted");
        }
      });
    }
    finally {
      currentLeaderStateLock.writeLock().unlock();
    }
  }

  public boolean firstCommitAsLeader() {
    return firstCommitAsLeader.get();
  }

  public void firstCommitAsLeader(boolean value) {
    firstCommitAsLeader.set(value);
  }

  public void handleConcurrentStateModificationException(ConcurrentStateModificationException e)
      throws InterruptedException, S2CStoppedException {
    currentLeaderStateLock.writeLock().lockInterruptibly();
    try {
      currentLeaderState(catchUpLeaderState());
      if (currentLeaderState() == null) {
        throw new IllegalStateException("Leader ClientState cannot be unknown at this node state",
            e);
      }
      if (currentLeaderState().getEpoch() <= e.leaderState().getEpoch()) {
        throw new IllegalStateException(
            "Command log or current leader state was modified, but leader epoch was not incremented.",
            e);
      }
      // Leader was changed correctly, notify
      notifyLeaderChange(currentLeaderState);
    }
    finally {
      currentLeaderStateLock.writeLock().unlock();
    }

  }

  public boolean isFollower(NodeIdentity nodeIdentity) {
    return followers.keySet().contains(nodeIdentity);
  }

  // Leader-only method
  public void updateCommitIndex(long commitIndex)
      throws ConcurrentStateModificationException, InterruptedException {

    Timer.Sample sample = Timer.start();
    currentLeaderStateLock.writeLock().lockInterruptibly();
    try {
      if (leaderETag == null) {
        throw new IllegalStateException(
            "Cannot update leader state while current leader etag is unknown");
      }

      LeaderState leaderState = LeaderState.newBuilder()
          .addAllFollowers(followers.values())
          .setNodeIdentity(nodeIdentity)
          .setCommitIndex(commitIndex)
          .setEpoch(currentLeaderState.getEpoch())
          .build();

      // We persist leader state as JSON to be human-readable
      String leaderStateJsonStr = leaderStateToJson(leaderState);
      ByteString leaderStateByteBuffer = ByteString
          .copyFrom(leaderStateJsonStr.getBytes(StandardCharsets.UTF_8));
      var newLeaderEtagOptional = objectWriter.writeIfMatch(keysResolver.leaderKey(),
          leaderStateByteBuffer, leaderETag);

      if (newLeaderEtagOptional.isEmpty()) {
        throw new ConcurrentStateModificationException(leaderState);
      } else {
        leaderETag = newLeaderEtagOptional.get();
        currentLeaderState = leaderState;
      }
    }
    finally {
      currentLeaderStateLock.writeLock().unlock();
      sample.stop(updateLatency);
    }
  }

  private LeaderState attemptLeadership(LeaderState current)
      throws InterruptedException, LowRankNodeException {

    // First epoch is 1
    long epoch = 1;
    long commitIndex = currentLeaderState == null ? 0L : current.getCommitIndex();
    LeaderState leaderState;

    Timer.Sample sample = Timer.start();
    if (current == null) {
      leaderState = LeaderState.newBuilder()
          .setNodeIdentity(nodeIdentity())
          .setCommitIndex(commitIndex)
          .setEpoch(epoch)
          .build();
      log.debug().addKeyValue("leaderState", leaderState).log("No current leader.");
    } else {

      log.debug()
          .addKeyValue("currentLeaderState", current)
          .log("Existing leader found. Checking health of higher ranked followers.");

      Set<FollowerInfo> followersCopy = current.getFollowersList()
          .stream()
          .collect(Collectors.toSet());

      if (!isLeader(current)) {

        // We add the node as a follower if it has just joined
        followersCopy.add(FollowerInfo.newBuilder()
            .setApplyIndex(applyIndexSupplier.get())
            .setNodeIdentity(nodeIdentity)
            .build());

        List<FollowerInfo> sortedFollowers = followersCopy.stream()
            .sorted(Comparator.comparingLong(f -> f.getApplyIndex()))
            .toList();

        int rank = verifyNoHigherRankAlive(sortedFollowers);

        log.debug().log("No healthy higher ranked followers found.");

        // The higher the rank, the less the delay
        long delay = leadershipDelay(followersCopy.size() - rank);

        log.debug()
            .addKeyValue("delay", delay)
            .log("Will attempt leadership after rank-based delay");

        // We give those with higher rank priority in case they are in another partition
        TimeUnit.MILLISECONDS.sleep(delay);
        // Start with no followers, as this is not leader currently
        leaderState = LeaderState.newBuilder()
            .setNodeIdentity(nodeIdentity())
            .setCommitIndex(current.getCommitIndex())
            .setEpoch(current.getEpoch() + 1)
            .build();

      } else {

        log.debug().log("Node is leader, no rank-based delay will be made");

        leaderState = LeaderState.newBuilder(current).setEpoch(current.getEpoch() + 1).build();

      }
    }

    log.debug()
        .addKeyValue("leaderState", () -> leaderStateToJson(leaderState))
        .log("Attempting leadership...");

    LeaderState newLeaderState = doAttemptLeadership(current, leaderState);

    sample.stop(attemptLatency);
    return newLeaderState;
  }

  private LeaderState catchUpLeaderState() throws InterruptedException, S2CStoppedException {
    verifyNotStopped();
    try {
      Timer.Sample sample = Timer.start();
      LeaderState leaderState = doCatchUpLeaderState();
      sample.stop(catchUpLatency);
      return leaderState;
    }
    catch (ObjectCorruptedException e) {
      throw new IllegalStateException("LeaderState couldn't be read", e);
    }
  }

  private LeaderState doAttemptLeadership(LeaderState currentLeaderState,
      LeaderState newLeaderState) throws InterruptedException {
    String leaderKey = keysResolver.leaderKey();

    Optional<String> newLeaderETag = Optional.empty();
    try {
      String leaderStateJsonStr = JsonFormat.printer().print(newLeaderState);
      ByteString leaderStateByteBuffer = ByteString
          .copyFrom(leaderStateJsonStr.getBytes(StandardCharsets.UTF_8));
      if (currentLeaderState != null) {
        if (leaderETag == null) {
          throw new IllegalStateException("Leader ETag cannot be null with at this node state");
        }
        newLeaderETag = objectWriter.writeIfMatch(leaderKey, leaderStateByteBuffer, leaderETag);
      } else {
        newLeaderETag = objectWriter.writeIfNoneMatch(leaderKey, leaderStateByteBuffer);
      }
      if (newLeaderETag.isPresent()) {
        leaderETag = newLeaderETag.get();
        log.debug().log("Leadership attempt ended successfully.");
        return newLeaderState;
      } else {
        log.debug().log("Leadership attempt failed");
      }
    }
    catch (InvalidProtocolBufferException e) {
      // Should never happen
      throw new IllegalStateException("Invalid leader state", e);
    }
    return currentLeaderState;
  }

  private LeaderState doCatchUpLeaderState() throws InterruptedException, ObjectCorruptedException {
    String leaderKey = keysResolver.leaderKey();

    log.trace().addKeyValue("key", leaderKey).log("Catching up leader state...");
    var result = objectReader.<LeaderState>readJson(leaderKey, LeaderState.newBuilder());
    if (result.isPresent()) {
      // We only re-assign if etag was changed
      if (!result.get().eTag().equals(leaderETag)) {
        leaderETag = result.get().eTag();
      }
      log.debug().addKeyValue("key", leaderKey).log("Leader state caught up successfully.");
      return result.get().object();
    }
    return null;

  }

  private void ensureValidLeaderState(String etagSnapshot)
      throws InterruptedException, S2CStoppedException {

    while (etagSnapshot == leaderETag) {

      verifyNotStopped();

      currentLeaderState(catchUpLeaderState());

      if (etagSnapshot == leaderETag) {
        try {
          currentLeaderState(attemptLeadership(currentLeaderState()));
        }
        catch (LowRankNodeException e) {
          // A node with higher rank exists, await while it can become
          // leader.
          TimeUnit.MILLISECONDS.sleep(leadershipDelay(followers.size() - e.highestRank()));
        }
      }
    }
  }

  private void initMetrics(String s2cGroupId, MeterRegistry meterRegistry) {
    updateLatency = Timer.builder("update.leader.latency")
        .tag("s2cGroupId", s2cGroupId)
        .register(meterRegistry);

    checkLatency = Timer.builder("check.leader.latency")
        .tag("s2cGroupId", s2cGroupId)
        .register(meterRegistry);

    catchUpLatency = Timer.builder("catchup.leader.latency")
        .tag("s2cGroupId", s2cGroupId)
        .register(meterRegistry);

    attemptLatency = Timer.builder("attempt.leader.latency")
        .tag("s2cGroupId", s2cGroupId)
        .register(meterRegistry);

  }

  // A predicate that is true if leader state is invalid
  private Predicate<LeaderState> invalidityPredicate(LeaderState oldLeaderState) {
    if (oldLeaderState == null) {
      return Objects::isNull;
      // We check reference because currentLeaderState is never mutated if not leader
    } else if (oldLeaderState == currentLeaderState()) {
      return ls -> ls.getEpoch() <= oldLeaderState.getEpoch();
    } else {
      return ls -> false;
    }
  }

  private boolean isAlive(FollowerInfo followerInfo) throws InterruptedException {
    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cOptions.s2cRetryOptions())
        .name("isAlive")
        .build();
    try (S2CClient s2cclient = s2cClientFactory.get()) {
      while (backoffCounter.canAttempt()) {
        try {
          s2cclient.connect(followerInfo.getNodeIdentity());
          return true;
        }
        catch (ConnectTimeoutException | ClientNotConnectedException | UnknownHostException
            | IOException e) {
          log.debug().setCause(e).log("Error while connecting");
        }
        if (backoffCounter.canAttempt()) {
          backoffCounter.enrich(log.debug()).log("Retrying");
          backoffCounter.awaitNextAttempt();
        }
      }
    }
    return false;
  }

  private long leadershipDelay(int rank) {
    return rank * (long) s2cOptions.leadershipDelay();
  }

  private String leaderStateToJson(LeaderState leaderState) {
    try {
      return JsonFormat.printer().alwaysPrintFieldsWithNoPresence().print(leaderState);
    }
    catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Updating leader state failed", e); // Should never happen
    }
  }

  private int verifyNoHigherRankAlive(List<FollowerInfo> sortedFollowers)
      throws InterruptedException, LowRankNodeException {
    int currentRank = 0;
    for (FollowerInfo follower : sortedFollowers) {
      if (follower.getNodeIdentity().equals(this.nodeIdentity())) {
        continue;
      }
      if (follower.getApplyIndex() < applyIndexSupplier.get()) {
        // The node we check has smaller applyIndex, so we have higher-rank
        currentRank++;
        // We don't need liveness-check, as we are highr-ranked
        continue;
      }
      // Now we have a node that has higher rank, we check for liveness
      if (isAlive(follower)) {
        log.debug()
            .addKeyValue("follower", follower)
            .log("An alive higher ranked follower found. Stepping back...");
        throw new LowRankNodeException(currentRank);
      }
    }
    return currentRank;
  }

  public boolean closed() {
    return stopped;
  }

  private LeaderState currentLeaderState() {
    return currentLeaderState;
  }

  private void currentLeaderState(LeaderState currentLeaderState) {
    this.currentLeaderState = currentLeaderState;
  }

  private NodeIdentity nodeIdentity() {
    return nodeIdentity;
  }

  private void verifyNotStopped() throws S2CStoppedException {
    if (closed()) {
      throw new S2CStoppedException();
    }
  }

  public LeaderState getLeaderState() throws InterruptedException, S2CStoppedException {
    return checkLeaderState(null);
  }

  public boolean isLeader(LeaderState leaderState) {
    return leaderState != null && leaderState.getNodeIdentity().equals(nodeIdentity());
  }

  public void resetLeaderState() throws InterruptedException, S2CStoppedException {
    currentLeaderStateLock.writeLock().lockInterruptibly();
    try {
      currentLeaderState = null;
      leaderETag = null;
      getLeaderState();
    }
    finally {
      currentLeaderStateLock.writeLock().unlock();
    }
  }
}