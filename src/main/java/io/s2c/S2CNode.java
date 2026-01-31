package io.s2c;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.concurrency.GuardedValue;
import io.s2c.concurrency.Sequencer;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.configs.S2COptions;
import io.s2c.error.S2CInterruptedException;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.StateRequest;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.ClientRole;
import io.s2c.network.S2CClient;
import io.s2c.network.S2CClient.ClientState;
import io.s2c.network.S2CGroupServer;
import io.s2c.network.S2CServer;
import io.s2c.network.error.ClientStoppedException;
import io.s2c.network.error.FatalServerException;
import io.s2c.network.message.reader.S2CMessageReader;
import io.s2c.s3.ObjectReader;
import io.s2c.s3.ObjectWriter;
import io.s2c.s3.S3Facade;
import io.s2c.util.LRUCache;

public class S2CNode implements AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(S2CNode.class);
  private final StructuredLogger log;
  private final NodeIdentity nodeIdentity;
  private final S2CLog s2cLog;
  private final LeaderStateManager leaderStateManager;
  private final S2CGroupServer s2cGroupServer;
  private final S2CClient s2cClient;
  private final String s2cGroupId;
  private final RSM rsm;
  private final SnapshottingWorker snapshottingWorker;
  private final LeaderHealthMonitor leaderHealthMonitor;
  private final NodeStateManager nodeStateManager;
  private final StateRequestHandler stateRequestHandler;
  private final ClientMessageHandler clientMessageHandler;
  private final TaskExecutor taskExecutor;
  private final SynchronizeManager synchronizeManager;
  private final AsyncBatchApplier asyncBatchApplier;
  private final StateRequestSubmitter stateRequestSubmitter;
  private final Supplier<S2CMessageReader> s2cMessageReaderFactory;
  private final ContextProvider contextProvider;
  private final S2COptions s2cOptions;

  private final MeterRegistry meterRegistry;
  private final AtomicInteger s2cClientsCount = new AtomicInteger();

  private final S2CGroupRegistry s2cGroupRegistry;
  private final S2CStateMachineRegistry s2cStateMachineRegistry;
  private final GuardedValue<Boolean> guardedLeaderStarting = new GuardedValue<>(false);

  private final GuardedValue<LRUCache<NodeIdentity, OrderedLastResult>> guardedNodesLastResult;

  private volatile boolean running = false;

  public static class Builder {
    private S2CServer s2cServer;
    private S3Facade s3Facade;
    private String bucket;
    private NodeIdentity nodeIdentity;
    private String s2cGroupId;
    private S2COptions s2cOptions;
    private S2CGroupRegistry s2cGroupRegistry;
    private Supplier<S2CMessageReader> s2cMessageReaderFactory;
    private MeterRegistry meterRegistry;

    public Builder s2cServer(S2CServer s2cServer) {
      this.s2cServer = s2cServer;
      return this;
    }

    public Builder bucket(String bucket) {
      this.bucket = bucket;
      if (bucket.isBlank()) {
        throw new IllegalArgumentException("'bucket' cannot be blank");
      }
      return this;
    }

    public Builder s3Facade(S3Facade s3Facade) {
      this.s3Facade = s3Facade;
      return this;
    }

    public Builder nodeIdentity(NodeIdentity nodeIdentity) {
      this.nodeIdentity = nodeIdentity;
      return this;
    }

    public Builder s2cGroupId(String s2cGroupId) {
      this.s2cGroupId = s2cGroupId;
      if (s2cGroupId.isBlank()) {
        throw new IllegalArgumentException("'s2cGroupId' cannot be blank");
      }
      return this;
    }

    public Builder s2cOptions(S2COptions s2cOptions) {
      this.s2cOptions = s2cOptions;
      return this;
    }

    public Builder s2cGroupRegistry(S2CGroupRegistry s2cGroupRegistry) {
      this.s2cGroupRegistry = s2cGroupRegistry;
      return this;
    }

    public Builder s2cMessageReaderFactory(Supplier<S2CMessageReader> s2cMessageReaderFactory) {
      this.s2cMessageReaderFactory = s2cMessageReaderFactory;
      return this;
    }
    
    public Builder meterRegistry(MeterRegistry meterRegistry) {
      this.meterRegistry = meterRegistry;
      return this;
    }

    public S2CNode build() {
      Objects.requireNonNull(s2cServer, "s2cServer must be set");
      Objects.requireNonNull(s3Facade, "'s3Facade' must be set");
      Objects.requireNonNull(bucket, "bucket must be set");
      Objects.requireNonNull(nodeIdentity, "nodeIdentity must be set");
      Objects.requireNonNull(s2cGroupId, "s2cGroupId must be set");

      if (s2cOptions == null) {
        s2cOptions = new S2COptions();
      }

      if (s2cMessageReaderFactory == null) {
        s2cMessageReaderFactory = () -> S2CMessageReader.create(s2cOptions.maxMessageSize());
      }

      if (s2cGroupRegistry == null) {
        s2cGroupRegistry = new S2CGroupRegistry();
      }
      
      if (meterRegistry == null) {
        meterRegistry = Metrics.globalRegistry;
      }

      return new S2CNode(s3Facade, s2cServer, bucket, nodeIdentity, s2cGroupId, s2cOptions,
          s2cMessageReaderFactory, s2cGroupRegistry, meterRegistry);

    }

  }

  private S2CNode(S3Facade s3Facade,
      S2CServer s2cServer,
      String bucket,
      NodeIdentity nodeIdentity,
      String s2cGroupId,
      S2COptions s2cOptions,
      Supplier<S2CMessageReader> s2cMessageReaderFactory,
      S2CGroupRegistry s2cGroupRegistry, MeterRegistry meterRegistry) {

    this.s2cGroupId = s2cGroupId;
    this.nodeIdentity = nodeIdentity;
    this.s2cOptions = s2cOptions;
    this.s2cGroupRegistry = s2cGroupRegistry;
    this.s2cMessageReaderFactory = s2cMessageReaderFactory;
    this.meterRegistry = meterRegistry;

    verifyNewGroup(s2cGroupId);

    guardedNodesLastResult = new GuardedValue<>(
        new LRUCache<>(s2cOptions.maxDeduplicatedClients(), k -> null));

    this.contextProvider = new ContextProvider(s2cGroupId, nodeIdentity, s2cOptions.logNodeIdentity());
    s2cStateMachineRegistry = new S2CStateMachineRegistry(this, this::submitStateRequest,
        this::nextSequenceNumber);

    this.log = new StructuredLogger(logger, contextProvider.loggingContext());

    taskExecutor = new TaskExecutor(contextProvider.ownerName(S2CNode.class),
        log.uncaughtExceptionLogger(), meterRegistry);

    Function<ClientRole, Supplier<S2CClient>> s2cClientCurriedFactory = newS2CClientCurriedFactory(
        s2cOptions);

    Function<StructuredLogger, ObjectReader> objectReaderFactory = l -> new ObjectReader(s3Facade,
        bucket, l, s2cOptions.s2cRetryOptions());

    Function<StructuredLogger, ObjectWriter> objectWriterFactory = l -> new ObjectWriter(s3Facade,
        bucket, l, s2cOptions.s2cRetryOptions());

    this.s2cLog = new S2CLog(objectReaderFactory, objectWriterFactory, contextProvider, s2cOptions,
        meterRegistry);

    this.s2cGroupServer = new S2CGroupServer(this::newClientMessageAcceptor, s2cGroupId,
        this::handleFatalServerException, s2cMessageReaderFactory, contextProvider, meterRegistry);

    s2cServer.registerS2CGroupServer(s2cGroupServer);

    SnapshotStorageManager snapshotStorageManager = new SnapshotStorageManager(objectReaderFactory,
        objectWriterFactory, contextProvider, meterRegistry);

    this.rsm = new RSM(contextProvider, s2cStateMachineRegistry, s2cLog::replay,
        snapshotStorageManager::download, guardedNodesLastResult);

    this.leaderStateManager = new LeaderStateManager(objectReaderFactory, objectWriterFactory,
        s2cOptions, contextProvider, s2cClientCurriedFactory.apply(ClientRole.IS_ALIVE_CHECKER),
        rsm::applyIndex, meterRegistry);

    this.synchronizeManager = new SynchronizeManager(s2cLog, s2cOptions, contextProvider,
        s2cClientCurriedFactory.apply(ClientRole.SYNCHRONIZER), leaderStateManager, meterRegistry);

    this.stateRequestHandler = new StateRequestHandler(contextProvider,
        s2cOptions.flushIntervalMs(), s2cOptions.batchMinCount(), leaderStateManager,
        synchronizeManager::syncCommit, rsm::applyBatch, s2cLog,
        leaderStateManager::handleConcurrentStateModificationException, guardedNodesLastResult,
        meterRegistry);

    this.snapshottingWorker = new SnapshottingWorker(leaderStateManager, contextProvider,
        snapshotStorageManager, rsm, leaderStateManager::handleConcurrentStateModificationException,
        this::cleanSnapshottedEntries, s2cOptions, meterRegistry);

    this.leaderHealthMonitor = new LeaderHealthMonitor(leaderStateManager,
        s2cOptions.maxMissedHeartbeats(), s2cOptions.leaderHeartbeatTimeoutMs(), contextProvider);

    s2cClient = s2cClientCurriedFactory.apply(ClientRole.FOLLOWER).get();

    this.nodeStateManager = new NodeStateManager(leaderStateManager, s2cClient, contextProvider,
        nodeIdentity, guardedLeaderStarting, rsm, this::cleanSnapshottedEntries, s2cOptions,
        meterRegistry);
    this.asyncBatchApplier = new AsyncBatchApplier(rsm, contextProvider, s2cOptions);
    this.clientMessageHandler = new ClientMessageHandler(contextProvider, leaderStateManager,
        leaderHealthMonitor::registerHeartbeat, guardedLeaderStarting, synchronizeManager,
        stateRequestHandler, asyncBatchApplier, rsm::applyIndex, this::tooFarBehindHanlder,
        leaderStateManager::handleConcurrentStateModificationException, ni -> {
          AtomicReference<Optional<Long>> seqNumOptionalReference = new AtomicReference<>();
          guardedNodesLastResult.read(nodesLastResults -> {
            var result = nodesLastResults.get(ni);
            if (result != null) {
              seqNumOptionalReference.set(Optional.of(result.lastResult().getLastSeqNum()));
            } else {
              seqNumOptionalReference.set(Optional.empty());
            }
          });
          return seqNumOptionalReference.get();
        }, meterRegistry);

    this.stateRequestSubmitter = new StateRequestSubmitter(nodeStateManager, leaderStateManager,
        clientMessageHandler::handleStateRequest, s2cClient, contextProvider, s2cOptions,
        meterRegistry);

    Gauge.builder("active.s2c.clients.count", s2cClientsCount::get)
        .description("The count of all active (connected) S2CClient instances")
        .register(meterRegistry);

  }

  private Function<ClientRole, Supplier<S2CClient>> newS2CClientCurriedFactory(
      S2COptions s2cOptions) {
    return clientRole -> () -> new S2CClient(s2cOptions, contextProvider, meterRegistry, s -> {
      if (s == ClientState.READY) {
        s2cClientsCount.incrementAndGet();
      } else {
        s2cClientsCount.decrementAndGet();
      }
    }, s2cMessageReaderFactory, clientRole);
  }

  public void start() throws InterruptedException {
    log.info().log("Starting S2CNode");
    running = true;
    try {
      leaderStateManager.getLeaderState(); // Trigger leader detection/attempt
      startBackgroundTasks();
    } catch (S2CStoppedException e) {
      log.error().setCause(e).log("Error while starting");
    }
  }

  public S2CMessage submitStateRequest(StateRequest stateRequest)
      throws InterruptedException, S2CStoppedException, ClientStoppedException {
    return stateRequestSubmitter.submit(stateRequest);
  }

  public <T extends S2CStateMachine> T createAndRegisterStateMachine(String name,
      Supplier<T> factory) {
    if (running) {
      throw new IllegalStateException("Cannot register state machine after node is started");
    }
    Objects.requireNonNull(name, "'name' cannot be null");
    if (name.isBlank()) {
      throw new IllegalArgumentException("'name' cannot be blank or empty");
    }
    T stateMachine = s2cStateMachineRegistry.createAndRegister(name, factory);
    if (stateMachine == null) {
      throw new IllegalArgumentException(
          "ClientState machine name %s is already registered".formatted(name));
    }
    return stateMachine;
  }

  @Override
  public void close() throws InterruptedException {
    running = false;
    log.info().log("Stopping S2CNode");
    leaderStateManager.close(); // Propagates S2CStoppedException
    stateRequestHandler.close();
    s2cGroupServer.close();
    taskExecutor.close();
    s2cLog.close();
    s2cClient.close();
    synchronizeManager.close();
  }

  public String s2cGroupId() {
    return s2cGroupId;
  }

  public NodeIdentity nodeIdentity() {
    return nodeIdentity;
  }

  public long commitIndex() throws InterruptedException, S2CStoppedException {
    return leaderStateManager.getLeaderState().getCommitIndex();
  }

  private long nextSequenceNumber() {
    try {
      return nodeStateManager.nextSequenceNumber();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      closeQuietly();
      throw new S2CInterruptedException(e);
    }
  }

  private void startBackgroundTasks() {
    taskExecutor.start("async-batch-applier", asyncBatchApplier);
    taskExecutor.start("node-state-manager", nodeStateManager);
    taskExecutor.start("state-requests-handler", stateRequestHandler);
    taskExecutor.start("snapshotting-worker", snapshottingWorker);
    taskExecutor.start("leader-health-monitor", leaderHealthMonitor);
  }

  private ClientMessageAcceptor newClientMessageAcceptor() {
    return new ClientMessageAcceptor(clientMessageHandler, contextProvider,
        s2cOptions.maxConcurrentStateRequestsHandling(),
        s2cOptions.s2cNetworkOptions().maxPendingRespsPerClient(), meterRegistry);
  }

  private void handleFatalServerException(S2CGroupServer server, FatalServerException e) {
    log.debug().setCause(e).log("Fatal server exception. Clearing up resources");
    closeQuietly();
  }

  private void closeQuietly() {
    try {
      close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug().setCause(e).log("Interrupted");
    }
  }

  private void cleanSnapshottedEntries(Long startSnapshotApplyIndex, Long endSnapshotApplyIndex) {
    try {
      s2cLog.truncate(startSnapshotApplyIndex, endSnapshotApplyIndex);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug().setCause(e).log("Interrupted");
    }
  }

  private void verifyNewGroup(String s2cGroupId) {
    if (s2cGroupRegistry.hasGroup(s2cGroupId) || !s2cGroupRegistry.registerGroup(s2cGroupId)) {
      throw new IllegalArgumentException(
          "S2C instance with s2cGroupId '%s' already exists: A running program (node) can have only one S2CNode per S2CGroup"
              .formatted(s2cGroupId));
    }
  }

  public boolean isLeader() throws InterruptedException, S2CStoppedException {
    return leaderStateManager.isLeader(leaderStateManager.getLeaderState());
  }

  public static Builder builder() {
    return new Builder();
  }

  public void tooFarBehindHanlder() {
    nodeStateManager.notifyTooFarBehind();
    try {
      leaderStateManager.resetLeaderState();
    } catch (S2CStoppedException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.debug().setCause(e).log("Error while resetting leader state");
      closeQuietly();
    }
  }
}