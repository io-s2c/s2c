package io.s2c;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.s2c.concurrency.GuardedValue;
import io.s2c.concurrency.RequestResponseTask;
import io.s2c.concurrency.Task;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.error.ApplicationException;
import io.s2c.error.ApplicationResultUnavailableException;
import io.s2c.error.ConcurrentStateModificationException;
import io.s2c.error.NotLeaderException;
import io.s2c.error.RequestOutOfSequenceException;
import io.s2c.error.S2CStoppedException;
import io.s2c.error.StateRequestException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.StateRequest;
import io.s2c.model.messages.StateRequest.StateRequestType;
import io.s2c.model.state.LastResult;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.LogEntriesBatch;
import io.s2c.model.state.LogEntry;
import io.s2c.model.state.NodeIdentity;
import io.s2c.model.state.RequestId;
import io.s2c.util.ConcurrentStateModificationExceptionHandler;
import io.s2c.util.LRUCache;

public class StateRequestHandler implements Task {

  public static record TraceableStateRequest(String correlationId,
      RequestResponseTask<StateRequest, ByteString, StateRequestException> reqRes) {
  }

  @FunctionalInterface
  interface BatchHandler {
    void accept(Set<TraceableStateRequest> batch) throws InterruptedException, S2CStoppedException;
  }

  private static final TraceableStateRequest POISON_PILL = new TraceableStateRequest(null, null);
  private Timer applicationLatencyCommand;
  private Timer applicationLatencyRead;
  private final Function<CommittedBatch, Long> batchApplier;
  private final int batchMinCount;

  private final BlockingQueue<TraceableStateRequest> commandsQueue = new LinkedBlockingQueue<>();
  private Counter committedBatches;
  private final ConcurrentStateModificationExceptionHandler concurrentStateModificationExceptionHandler;
  private final int flushIntervalMs;
  private Counter handledBatches;
  private Timer readHandleLatency;
  private Timer commandHandleLatency;
  private final LeaderStateManager leaderStateManager;
  private final StructuredLogger log;
  private final ContextProvider contextProvider;
  private final ReentrantLock handleLock = new ReentrantLock();
  private final Logger logger = LoggerFactory.getLogger(StateRequestHandler.class);
  private final TaskExecutor taskExecutor;

  private MeterRegistry meterRegistry;
  private final BlockingQueue<TraceableStateRequest> readsQueue = new LinkedBlockingQueue<>();

  private volatile boolean running = false;
  private final S2CLog s2cLog;

  private final Consumer<Long> synchronizer;

  private final GuardedValue<LRUCache<NodeIdentity, OrderedLastResult>> guardedNodesLastResults;

  public StateRequestHandler(ContextProvider contextProvider,
      int flushIntervalMs,
      int batchMinCount,
      LeaderStateManager leaderStateManager,
      Consumer<Long> synchronizer,
      Function<CommittedBatch, Long> batchApplier,
      S2CLog s2cLog,
      ConcurrentStateModificationExceptionHandler concurrentStateModificationExceptionHandler,
      GuardedValue<LRUCache<NodeIdentity, OrderedLastResult>> guardedNodesLastResults,
      MeterRegistry meterRegistry) {
    this.contextProvider = contextProvider;
    log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.batchMinCount = batchMinCount;
    this.flushIntervalMs = flushIntervalMs;
    this.leaderStateManager = leaderStateManager;
    this.batchApplier = batchApplier;
    this.synchronizer = synchronizer;
    this.s2cLog = s2cLog;
    this.concurrentStateModificationExceptionHandler = concurrentStateModificationExceptionHandler;
    this.taskExecutor = new TaskExecutor(contextProvider.ownerName(StateRequestHandler.class),
        log.uncaughtExceptionLogger(), meterRegistry);
    this.guardedNodesLastResults = guardedNodesLastResults;
    this.meterRegistry = meterRegistry;
    initMetrics(contextProvider, meterRegistry);

  }

  @Override
  public void close() throws InterruptedException {
    running = false;
    taskExecutor.close();
  }

  public ByteString handle(String correlationId, StateRequest stateRequest)
      throws StateRequestException, InterruptedException, RequestOutOfSequenceException {

    TraceableStateRequest req = new TraceableStateRequest(correlationId,
        new RequestResponseTask<>(stateRequest));
    try {

      ByteString response = null;
      Timer.Sample sample = null;
      if (stateRequest.getType() == StateRequestType.COMMAND) {
        AtomicBoolean accepted = new AtomicBoolean(false);
        AtomicBoolean outOfSeq = new AtomicBoolean(false);
        AtomicLong nextSeqNum = new AtomicLong();
        guardedNodesLastResults.write(nodesLastResults -> {
          OrderedLastResult lastResult = nodesLastResults
              .get(req.reqRes().request().getSourceNode());
          if (lastResult != null) {
            if (lastResult.lastResult().getLastSeqNum() == 0) {
              // Received but not processed yet
              nextSeqNum.set(lastResult.nextSeqNum());
              outOfSeq.set(true);
            } else if (req.reqRes().request().getSequenceNumber() == lastResult.lastResult()
                .getLastSeqNum()) {
              if (!lastResult.lastResult().getErrMsg().equals(RSM.NO_ERR_MSG)) {
                req.reqRes()
                    .exception(new ApplicationException(lastResult.lastResult().getErrMsg()));
              } else {
                req.reqRes().response(lastResult.lastResult().getResult());
              }
            } else if (req.reqRes().request().getSequenceNumber() < lastResult.lastResult()
                .getLastSeqNum()) {
              req.reqRes().exception(new ApplicationResultUnavailableException());
            } else if (req.reqRes().request().getSequenceNumber() != lastResult.nextSeqNum()) {
              outOfSeq.set(true);
              nextSeqNum.set(lastResult.nextSeqNum());
              log.debug()
                  .addKeyValue("seqNum", req.reqRes().request().getSequenceNumber())
                  .addKeyValue("nextSeqNum", lastResult.nextSeqNum())
                  .log("Skipping out of sequence request");
            } else {
              lastResult.nextSeqNum(req.reqRes().request().getSequenceNumber() + 1);
              accepted.set(true);
            }
          } else {
            // First request from a client must have seqNum=1
            if (req.reqRes().request().getSequenceNumber() != 1L) {
              nextSeqNum.set(1);
              outOfSeq.set(true);
            } else {
              nodesLastResults.put(req.reqRes().request().getSourceNode(),
                  new OrderedLastResult(LastResult.newBuilder().setLastSeqNum(0).build()));
              accepted.set(true);
            }
          }
          return nodesLastResults;
        });
        if (outOfSeq.get()) {
          throw new RequestOutOfSequenceException(nextSeqNum.get());
        }
        if (accepted.get()) {

          commandsQueue.put(req);
        }
        sample = Timer.start();
        response = req.reqRes.await();

      } else {
        readsQueue.put(req);
        sample = Timer.start();
        response = req.reqRes.await();
      }
      sample.stop(timer(stateRequest.getType()));
      return response;
    }
    catch (InterruptedException e) {
      log.debug().setCause(e).log("Interrupted");
      close();
      Thread.currentThread().interrupt();
      throw e;
    }

  }

  private Timer timer(StateRequestType stateRequestType) {
    return stateRequestType == StateRequestType.COMMAND ? commandHandleLatency : readHandleLatency;
  }

  @Override
  public void init() {
    running = true;
  }

  @Override
  public void run() {

    taskExecutor.start("commands-handler",
        Task.of(() -> startBatchingLoop(commandsQueue, this::handleCommands),
            () -> commandsQueue.put(POISON_PILL)));

    taskExecutor.start("reads-handler", Task.of(
        () -> startBatchingLoop(readsQueue, this::handleReads), () -> readsQueue.put(POISON_PILL)));

    try {
      taskExecutor.join();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug().setCause(e).log("Interrupted");
      closeQuietly();
    }

  }

  private void closeQuietly() {
    try {
      close();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug().setCause(e).log("Error while closing");
    }
  }

  private void handleCommands(Set<TraceableStateRequest> commands)
      throws InterruptedException, S2CStoppedException {
    LeaderState leaderState = leaderStateManager.getLeaderState();
    if (!leaderStateManager.isLeader(leaderState)) {
      log.debug().log("Cannot flush batch as node is not leader.");
      commands.forEach(t -> t.reqRes.exception(new NotLeaderException(leaderState)));
      return;
    }
    log.trace().log("Handling pending batched state requests");
    LogEntriesBatch.Builder commandsBatchBuilder = LogEntriesBatch.newBuilder();
    commands.forEach(c -> {

      if (c.reqRes().request().getSequenceNumber() == 1
          && c.reqRes().request().getSourceNode().getPort() == 7777) {
        log.info();
      }

      RequestId requestId = RequestId.newBuilder()
          .setClientNodeIdentity(c.reqRes().request().getSourceNode())
          .setClientSequenceNumber(c.reqRes().request().getSequenceNumber())
          .build();
      LogEntry entryProto = LogEntry.newBuilder()
          .setBody(c.reqRes().request().getBody())
          .setSourceSm(c.reqRes().request().getSourceSm())
          .setRequestId(requestId)
          .build();
      commandsBatchBuilder.addLogEntries(entryProto);
    });
    try {

      long commitIndex = leaderState.getCommitIndex();
      if (!commands.isEmpty()) {
        // Update leader state before commit
        boolean committed = false;
        if (leaderStateManager.firstCommitAsLeader()) {

          try {
            s2cLog.append(commandsBatchBuilder.setCommitIndex(commitIndex).build(), commitIndex,
                leaderState);
            committedBatches.increment();
            committed = true;
            leaderStateManager.firstCommitAsLeader(false);
          }
          catch (ConcurrentStateModificationException e) {
            concurrentStateModificationExceptionHandler.accept(e);
          }
        }
        if (!committed) {
          commitIndex++;
          leaderStateManager.updateCommitIndex(commitIndex);
          s2cLog.append(commandsBatchBuilder.setCommitIndex(commitIndex).build(), commitIndex,
              leaderState);
          committedBatches.increment();
        }
        log.trace()
            .addKeyValue("firstCommitAsLeader", leaderStateManager.firstCommitAsLeader())
            .addKeyValue("commitIndex", commitIndex)
            .log("Committed");
        Timer.Sample sample = Timer.start();

        long applyIndex = batchApplier
            .apply(new CommittedBatch(commands, StateRequestType.COMMAND, commitIndex));

        if (applyIndex != leaderStateManager.getLeaderState().getCommitIndex()) {
          throw new IllegalStateException(
              "applyIndex (%d) and commitIndex (%d) don't match after commit applied"
                  .formatted(applyIndex, leaderStateManager.getLeaderState().getCommitIndex()));
        }
        sample.stop(applicationLatencyCommand);
        // Synchronize async to followers
        synchronizer.accept(commitIndex);
        log.trace()
            .addKeyValue("commitIndex", commitIndex)
            .addKeyValue("applyIndex", applyIndex)
            .log("Batch processed");
      }
    }
    catch (ConcurrentStateModificationException e) {

      commands.forEach(c -> {
        c.reqRes().exception(e);
      });
      concurrentStateModificationExceptionHandler.accept(e);

      guardedNodesLastResults.write(nodesLastResults -> {

        commands.stream()
            .collect(Collectors.groupingBy(c -> c.reqRes().request().getSourceNode()))
            .entrySet()
            .forEach(entry -> {
              OrderedLastResult lastResult = nodesLastResults.get(entry.getKey());
              if (lastResult != null) {
                entry.getValue()
                    .stream()
                    .sorted(Comparator.comparingLong(t -> t.reqRes().request().getSequenceNumber()))
                    .findFirst()
                    .ifPresent(t -> {
                      lastResult.nextSeqNum(t.reqRes().request().getSequenceNumber());
                    });
              }
            });

        return nodesLastResults;

      });

    }
    finally {
      handledBatches.increment();
    }
  }

  private void handleReads(Set<TraceableStateRequest> reads)
      throws InterruptedException, S2CStoppedException {
    Set<TraceableStateRequest> copyReads = reads.stream()
        .map(r -> new TraceableStateRequest(r.correlationId(),
            new RequestResponseTask<>(r.reqRes().request())))
        .collect(Collectors.toSet());
    LeaderState leaderState = leaderStateManager.getLeaderState();
    if (!leaderStateManager.isLeader(leaderState)) {
      log.debug().log("Cannot flush batch as node is not leader.");
      reads.forEach(t -> t.reqRes.exception(new NotLeaderException(leaderState)));
      return;
    }
    long commitIndex = leaderState.getCommitIndex();
    Timer.Sample sample = Timer.start();
    batchApplier.apply(new CommittedBatch(copyReads, StateRequestType.READ, commitIndex));
    sample.stop(applicationLatencyRead);
    // Update leader state to ensure still leader - commitIndex unchanged
    try {
      // Only if we ensured we're still leader, we respond to the original read
      // requests
      leaderStateManager.updateCommitIndex(commitIndex);

      Map<String, TraceableStateRequest> readsMap = reads.stream()
          .collect(Collectors.toMap(r -> r.correlationId(), Function.identity()));

      copyReads.forEach(req -> {
        var originalRead = readsMap.get(req.correlationId());
        if (req.reqRes().excption() != null) {
          originalRead.reqRes().exception(req.reqRes().excption());
        } else {
          originalRead.reqRes().response(req.reqRes().response());
        }
      });
      log.trace().log("Batch of read requests applied.");
    }
    catch (ConcurrentStateModificationException e) {
      reads.forEach(r -> r.reqRes().exception(e));
      concurrentStateModificationExceptionHandler.accept(e);
    }
    finally {
      handledBatches.increment();
    }
  }

  private void initMetrics(ContextProvider contextProvider, MeterRegistry meterRegistry) {

    this.commandHandleLatency = Timer.builder("state.request.handle.latency")
        .tag("state.request.type", "command")
        .description("The latency between enqueueing a state request and receiving a response")
        .register(meterRegistry);

    this.readHandleLatency = Timer.builder("state.request.handle.latency")
        .tag("state.request.type", "read")
        .description("The latency between enqueueing a state request and receiving a response")
        .register(meterRegistry);

    this.applicationLatencyCommand = Timer.builder("state.request.application.latency")
        .tag("state.request.type", "command")
        .description("The latency of the application of the request by the state machine")
        .register(meterRegistry);

    this.applicationLatencyRead = Timer.builder("state.request.application.latency")
        .tag("state.request.type", "read")
        .description("The latency of the application of the request by the state machine")
        .register(meterRegistry);

    this.committedBatches = Counter.builder("state.request.committed.batches.count")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .description("The count of committed batches since the handler started")
        .register(meterRegistry);

    this.handledBatches = Counter.builder("state.request.handled.batches.count")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .description("The count of handled batches since the handler started")
        .register(meterRegistry);
  }

  private long remainingWaitTime(long startTime) {
    long elapsedTime = System.nanoTime() - startTime;
    return TimeUnit.MILLISECONDS.toNanos(flushIntervalMs) - elapsedTime;
  }

  private void startBatchingLoop(BlockingQueue<TraceableStateRequest> requestsQueue,
      BatchHandler batchHandler) {

    Set<TraceableStateRequest> batch = new HashSet<>();
    while (running) {

      long startTime = System.nanoTime();
      while (true) {
        long remainingWaitTime = remainingWaitTime(startTime);
        try {
          if (contextProvider.nodeIdentity().getPort() == 7777) {
            log.info();
          }
          var req = requestsQueue.poll(remainingWaitTime, TimeUnit.NANOSECONDS);

          if (req == POISON_PILL) {
            running = false;
            break;
          }
          if (req != null) {
            batch.add(req);
          }
          if (remainingWaitTime <= 0 || batch.size() >= batchMinCount) {
            if (!batch.isEmpty()) {
              log.trace()
                  .addKeyValue("timeAwaited", timeAwaited(remainingWaitTime))
                  .addKeyValue("batchCount", batch.size())
                  .log("Batch ready.");

              if (handleLock.tryLock()) {
                try {
                  batchHandler.accept(batch);
                  batch.clear();
                  break;
                }
                finally {
                  handleLock.unlock();
                }
              } // Otherwise accumulate more while waiting

            }
          }
        }
        catch (InterruptedException | S2CStoppedException e) {
          log.debug().setCause(e).log("Error while handling batch");
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
          closeQuietly();
          return;
        }
      }
    }
  }

  private long timeAwaited(long remainingTime) {
    return TimeUnit.NANOSECONDS
        .toMillis(TimeUnit.MILLISECONDS.toNanos(flushIntervalMs) - remainingTime);
  }

}