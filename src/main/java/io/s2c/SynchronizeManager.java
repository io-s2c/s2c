package io.s2c;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.configs.S2COptions;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.FollowerInfo;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.S2CClient;

public class SynchronizeManager implements AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(SynchronizeManager.class);
  private final StructuredLogger log;
  private final Map<NodeIdentity, FollowerSynchronizer> synchronizers = new ConcurrentHashMap<>();
  private final S2CLog s2cLog;
  private final S2COptions s2cOptions;
  private final TaskExecutor taskExecutor;
  private final ContextProvider contextProvider;
  private final Supplier<S2CClient> s2cClientFactory;
  private final LeaderStateManager leaderStateManager;
  private final MeterRegistry meterRegistry;

  public SynchronizeManager(S2CLog s2cLog,
      S2COptions s2cOptions,
      ContextProvider contextProvider,
      Supplier<S2CClient> s2cClientFactory,
      LeaderStateManager leaderStateManager,
      MeterRegistry meterRegistry) {
    this.s2cLog = s2cLog;
    this.s2cOptions = s2cOptions;
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.taskExecutor = new TaskExecutor(contextProvider.ownerName(SynchronizeManager.class),
        log.uncaughtExceptionLogger(), meterRegistry);
    this.contextProvider = contextProvider;
    this.leaderStateManager = leaderStateManager;
    this.s2cClientFactory = s2cClientFactory;
    this.meterRegistry = meterRegistry;
    Gauge.builder("s2c.synchronizers.count", synchronizers::size)
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .description("The count of active followers' synchronizers")
        .register(meterRegistry);
  }

  public void syncFollower(FollowerInfo followerInfo) {
    FollowerSynchronizer synchronizer = new FollowerSynchronizer(followerInfo, s2cLog,
        f -> synchronizers.remove(f.getNodeIdentity()), s2cOptions, contextProvider,
        s2cClientFactory, leaderStateManager, meterRegistry);
    taskExecutor.start("%s-synchronizer".formatted(followerInfo.getNodeIdentity().getId()),
        synchronizer);
    synchronizers.put(followerInfo.getNodeIdentity(), synchronizer);
  }

  public void syncCommit(Long commitIndex) {
    synchronizers.values().forEach(s -> s.syncToCommitIndex(commitIndex));
  }

  @Override
  public void close() throws InterruptedException {
    synchronizers.values().forEach(s -> {
      try {
        s.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e).log("Interrupted");
      }
    });
    taskExecutor.close();
  }

}
