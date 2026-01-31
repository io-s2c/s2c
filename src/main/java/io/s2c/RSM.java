package io.s2c;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.s2c.LogReplayer.ReplayerBrokenException;
import io.s2c.StateRequestHandler.TraceableStateRequest;
import io.s2c.concurrency.GuardedValue;
import io.s2c.error.ApplicationException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.StateRequest.StateRequestType;
import io.s2c.model.state.LastResult;
import io.s2c.model.state.LogEntriesBatch;
import io.s2c.model.state.LogEntry;
import io.s2c.model.state.NodeIdentity;
import io.s2c.model.state.StateMachineSnapshot;
import io.s2c.model.state.StateSnapshot;
import io.s2c.util.InterruptableSupplier;
import io.s2c.util.LRUCache;

public class RSM {
  
  public static final String NO_ERR_MSG = "No_S2C_Err_Msg";

  private final Logger logger = LoggerFactory.getLogger(RSM.class);
  private final StructuredLogger log;
  private final ReentrantLock applyLock = new ReentrantLock();
  private final Condition applyCondition = applyLock.newCondition();
  private final S2CStateMachineRegistry s2cStateMachineRegistry;
  private final BiFunction<Long, Long, LogReplayer> logReplayerFactory;
  private final InterruptableSupplier<Optional<StateSnapshot>> stateSnapshotSupplier;
  private final GuardedValue<LRUCache<NodeIdentity, OrderedLastResult>> guardedNodesLastResults;
  
  // applyIndex is incremented only when applyLock is acquired, regardless of node's role.
  // As with commitIndex, it is incremented per batch.
  private volatile long applyIndex = 0;

  public RSM(ContextProvider contextProvider,
      S2CStateMachineRegistry s2cStateMachineRegistry,
      BiFunction<Long, Long, LogReplayer> logReplayerFactory,
      InterruptableSupplier<Optional<StateSnapshot>> stateSnapshotSupplier,
      GuardedValue<LRUCache<NodeIdentity, OrderedLastResult>> guardedNodesLastResults) {
    log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.s2cStateMachineRegistry = s2cStateMachineRegistry;
    this.logReplayerFactory = logReplayerFactory;
    this.stateSnapshotSupplier = stateSnapshotSupplier;
    this.guardedNodesLastResults = guardedNodesLastResults;

  }

  public Optional<StateSnapshot> catchUp(long commitIndex) throws InterruptedException, ReplayerBrokenException {
    applyLock.lockInterruptibly();
    try {
      log.debug().addKeyValue("commitIndex", commitIndex).log("Catching up to commitIndex");
      var stateSnapshotOptional = restore();
      if (applyIndex < commitIndex) {
        log.debug().log("Applying committed log entries ahead of applyIndex");
        // applyIndex + 1 because applyIndex is applied already
        try (var logPlayer = logReplayerFactory.apply(applyIndex + 1, commitIndex)) {
          applyFromReplayer(logPlayer);
        }
      }
      // last leader might have incremented commitIndex before appending to log
      if (applyIndex < commitIndex - 1) {
        throw new IllegalStateException("applyIndex didn't catch to commitIndex");
      }
      log.debug().addKeyValue("applyIndex", applyIndex).log("Finsihed catching up");
      return stateSnapshotOptional;
    }
    finally {
      applyLock.unlock();
    }
  }

  private void applyFromReplayer(LogReplayer logReplayer)
      throws InterruptedException, ReplayerBrokenException {
    var next = logReplayer.next();
    while (next != null) {
      doApplyLogEntriesBatchQuietly(next);
      next = logReplayer.next();
    }
  }

  // This is never called concurrently, but lock to apply only if not catching up. Otherwise do
  // nothing.
  public boolean applyLogEntriesBatchQuietly(LogEntriesBatch next) {
    if (applyLock.tryLock()) {
      try {
        if (applyIndex + 1 == next.getCommitIndex()) {
          doApplyLogEntriesBatchQuietly(next);
          return true;
        }
      }
      finally {
        applyLock.unlock();
      }
    }
    return false;
  }

  private void doApplyLogEntriesBatchQuietly(LogEntriesBatch next) {

    if (next.getCommitIndex() != applyIndex + 1) {
      throw new IllegalStateException("Batch out of sequence");
    }

    for (LogEntry entry : next.getLogEntriesList()) {
      LastResult.Builder lastResultBuilder = LastResult.newBuilder();
      try {
        ByteString result = apply("", entry.getSourceSm(), entry.getBody(),
            StateRequestType.COMMAND);
        lastResultBuilder.setResult(result);
        lastResultBuilder.setErrMsg(NO_ERR_MSG);
      }
      catch (ApplicationException e) {
        log.debug().setCause(e).log("Error while applying");
        lastResultBuilder.setErrMsg(e.getMessage());
      }

      LastResult newLastResult = lastResultBuilder
          .setNodeIdentity(entry.getRequestId().getClientNodeIdentity())
          .setLastSeqNum(entry.getRequestId().getClientSequenceNumber())
          .build();

      guardedNodesLastResults.write(nodesLastResults -> {

        OrderedLastResult currentLastResult = nodesLastResults
            .get(entry.getRequestId().getClientNodeIdentity());

        if (currentLastResult != null) {
          if (currentLastResult.lastResult().getLastSeqNum() < newLastResult.getLastSeqNum()) {
            nodesLastResults.put(entry.getRequestId().getClientNodeIdentity(),
                new OrderedLastResult(newLastResult));
          }
        } else {
          nodesLastResults.put(entry.getRequestId().getClientNodeIdentity(),
              new OrderedLastResult(newLastResult));

        }

        return nodesLastResults;
      });
    }
    applyIndex++;
  }

  public Long applyBatch(CommittedBatch batch) {
    applyLock.lock();
    try {

      doApplyBatch(batch.batch());
      if (batch.requestsType() == StateRequestType.COMMAND) {
        if (batch.commitIndex() != applyIndex + 1) {
          throw new IllegalStateException("Batch out of sequence");
        }
        applyCondition.signalAll();
        applyIndex++;
      }
      return applyIndex;
    }
    finally {
      applyLock.unlock();
    }
  }
  
  public void awaitApply() throws InterruptedException {
    applyLock.lockInterruptibly();
    long applyIndexBeforeAwait = applyIndex;
    try {
      while (applyIndexBeforeAwait == applyIndex) {
        applyCondition.await();
      }
    }
    finally {
      applyLock.unlock();
    }
  }

  public StateSnapshot takeSnapshot(long lastSnapshotEndApplyIndex, long leaderEpoch) throws InterruptedException {
    applyLock.lock();
    try {
      StateSnapshot.Builder snapshotBuilder = StateSnapshot.newBuilder();
      log.debug().addKeyValue("applyIndex", applyIndex).log("Taking snapshot");
      snapshotBuilder.setLeaderEpoch(leaderEpoch)
      .setStartApplyIndex(lastSnapshotEndApplyIndex + 1)
      .setEndApplyIndex(applyIndex);
      s2cStateMachineRegistry.getAll().forEach(e -> {
        ByteString snapshotByteStr = e.snapshot();
        StateMachineSnapshot snapshot = StateMachineSnapshot.newBuilder()
            .setBody(snapshotByteStr)
            .setSmName(e.name())
            .build();
        snapshotBuilder.addSmSnapshots(snapshot);
      });

      guardedNodesLastResults.write(nodesLastResults -> {
        nodesLastResults.applyOnIterator(it -> {
          while (it.hasNext()) {
            var orderedLastResult = it.next().getValue();
            snapshotBuilder.addLastResults(orderedLastResult.lastResult());
          }
        });
        return nodesLastResults;
      });

      log.debug().addKeyValue("applyIndex", applyIndex).log("Snapshot ready");
      return snapshotBuilder.build();
    }
    finally {
      applyLock.unlock();
    }
  }

  private long applySnapshot(StateSnapshot stateSnapshot) throws InterruptedException {

    guardedNodesLastResults.write(nodesLastResults -> {
      stateSnapshot.getLastResultsList().forEach(lastResult -> {
        nodesLastResults.put(lastResult.getNodeIdentity(), new OrderedLastResult(lastResult));
      });
      return nodesLastResults;
    });
    for (StateMachineSnapshot smSnapshot : stateSnapshot.getSmSnapshotsList()) {
      S2CStateMachine s2cStateMachine = s2cStateMachineRegistry.get(smSnapshot.getSmName());
      if (s2cStateMachine == null) {
        log.warn()
            .addKeyValue("stateMachineName", smSnapshot.getSmName())
            .log("ClientState machine registry has no state machine "
                + "with such name. Skipping state machine snapshot");
      } else {
        s2cStateMachine.loadSnapshot(smSnapshot.getBody());
      }
    }
    applyIndex = stateSnapshot.getEndApplyIndex();
    return applyIndex;
  }

  public long applyIndex() {
    applyLock.lock();
    try {
      return applyIndex;
    }
    finally {
      applyLock.unlock();
    }
  }

  private void doApplyBatch(Collection<TraceableStateRequest> batch) {
    for (var t : batch) {

      try {
        ByteString result = apply(t.correlationId(), t.reqRes().request().getSourceSm(),
            t.reqRes().request().getBody(), t.reqRes().request().getType());
        log.trace().addKeyValue("correlationId", t.correlationId()).log("Request applied");

        t.reqRes().response(result);
      }
      catch (ApplicationException e) {
        log.debug().setCause(e).addKeyValue("correlationId", t.correlationId()).log("Error while applying request");
        t.reqRes().exception(e);
      }

      if (t.reqRes().request().getType() == StateRequestType.COMMAND) {
        LastResult.Builder lastResultBuilder = LastResult.newBuilder();

        lastResultBuilder.setNodeIdentity(t.reqRes().request().getSourceNode())
            .setLastSeqNum(t.reqRes().request().getSequenceNumber());
        if (t.reqRes().excption() != null) {
          lastResultBuilder.setErrMsg(t.reqRes().excption().getMessage());
        } else {
          lastResultBuilder.setResult(t.reqRes().response());
          lastResultBuilder.setErrMsg(NO_ERR_MSG);
        }

        LastResult newLastResult = lastResultBuilder.build();

        guardedNodesLastResults.write(nodesLastResults -> {
          OrderedLastResult currentLastResult = nodesLastResults
              .get(t.reqRes().request().getSourceNode());
          if (currentLastResult != null) {
            if (currentLastResult.lastResult().getLastSeqNum() < newLastResult.getLastSeqNum()) {
              nodesLastResults.put(t.reqRes().request().getSourceNode(),
                  new OrderedLastResult(newLastResult));
            }
          } else {
            nodesLastResults.put(t.reqRes().request().getSourceNode(),
                new OrderedLastResult(newLastResult));
          }
          return nodesLastResults;
        });
      }
    }
  }

  private ByteString apply(String correlationId, String stateMachineName, ByteString requestBody,
      StateRequestType stateRequestType) throws ApplicationException {
    S2CStateMachine stateMachine = s2cStateMachineRegistry.get(stateMachineName);
    if (stateMachine == null) {
      // This should never happen, as state machine creation must be made
      // by createOrRegisterStateMachine, otherwise it will throw by calling
      // apply() on the state machine object itself.
      throw new IllegalStateException("Unknown stateMachineName %s".formatted(stateMachineName));
    }
    log.trace()
        .addKeyValue("correlationId", correlationId)
        .addKeyValue("stateMachineName", stateMachineName)
        .log("Delegating request to state machine");

    return stateMachine.handleRequest(requestBody, stateRequestType);
  }

  private Optional<StateSnapshot> restore() throws InterruptedException {
    log.trace().log("Restoring snapshot");
    Optional<StateSnapshot> stateSnapshotOptional = stateSnapshotSupplier.get();
    if (stateSnapshotOptional.isEmpty()) {
      log.debug().log("No valid snapshot.");
      return stateSnapshotOptional;
    }
    
    StateSnapshot snapshot = stateSnapshotOptional.get();
    
    if (snapshot.getEndApplyIndex() <= applyIndex) {
      log.debug()
          .addKeyValue("applyIndex", applyIndex)
          .addKeyValue("snapshotApplyIndex", snapshot.getEndApplyIndex())
          .log("Node applyIndex is ahead snapshot. Skipping.");
      return stateSnapshotOptional;
    }
    log.debug()
        .addKeyValue("applyIndex", applyIndex)
        .addKeyValue("snapshotApplyIndex", snapshot.getEndApplyIndex())
        .log("Applying snapshot");
    applySnapshot(snapshot);
    log.debug().addKeyValue("applyIndex", applyIndex).log("Snapshot applied.");
    return stateSnapshotOptional;
  }
}