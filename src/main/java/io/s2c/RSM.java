package io.s2c;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.s2c.LogReplayer.ReplayerBrokenException;
import io.s2c.StateRequestHandler.TraceableStateRequest;
import io.s2c.concurrency.GuardedValue;
import io.s2c.error.ApplicationException;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.InternalStateRequest;
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
  private final GuardedValue<LRUCache<NodeIdentity, DedupUnit>> guardedNodesDedups;
  private final InternalStateRequestHandler internalStateRequestHandler;

  // applyIndex is incremented only when applyLock is acquired, regardless of node's role.
  // As with commitIndex, it is incremented per batch.
  private volatile long applyIndex = 0;

  public RSM(ContextProvider contextProvider,
      S2CStateMachineRegistry s2cStateMachineRegistry,
      BiFunction<Long, Long, LogReplayer> logReplayerFactory,
      InterruptableSupplier<Optional<StateSnapshot>> stateSnapshotSupplier,
      GuardedValue<LRUCache<NodeIdentity, DedupUnit>> guardedNodesLastResults,
      InternalStateRequestHandler internalStateRequestHandler) {
    log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.s2cStateMachineRegistry = s2cStateMachineRegistry;
    this.logReplayerFactory = logReplayerFactory;
    this.stateSnapshotSupplier = stateSnapshotSupplier;
    this.guardedNodesDedups = guardedNodesLastResults;
    this.internalStateRequestHandler = internalStateRequestHandler;
  }

  public Optional<StateSnapshot> catchUp(long commitIndex)
      throws InterruptedException, ReplayerBrokenException {
    applyLock.lockInterruptibly();
    try {
      log.debug()
          .addKeyValue("commitIndex", commitIndex)
          .log("Catching up to commitIndex");
      var stateSnapshotOptional = restoreSnapshot();
      if (applyIndex < commitIndex) {
        log.debug()
            .log("Applying committed log entries ahead of applyIndex");
        // applyIndex + 1 because applyIndex is applied already
        try (var logPlayer = logReplayerFactory.apply(applyIndex + 1, commitIndex)) {
          applyFromReplayer(logPlayer);
        }
      }
      // last leader might have incremented commitIndex before appending to log
      if (applyIndex < commitIndex - 1) {
        throw new IllegalStateException("applyIndex didn't catch to commitIndex");
      }
      log.debug()
          .addKeyValue("applyIndex", applyIndex)
          .log("Finsihed catching up");
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
        ByteString result = apply("",
            entry.getSourceSm(),
            entry.getBody(),
            StateRequestType.COMMAND,
            entry.getInternal());
        lastResultBuilder.setResult(result);
        lastResultBuilder.setErrMsg(NO_ERR_MSG);
      }
      catch (ApplicationException e) {
        log.debug()
            .setCause(e)
            .log("Error while applying");
        lastResultBuilder.setResult(ByteString.EMPTY);
        lastResultBuilder.setErrMsg(e.getMessage());
      }

      LastResult newLastResult = lastResultBuilder.setNodeIdentity(entry.getRequestId()
          .getClientNodeIdentity())
          .setLastSeqNum(entry.getRequestId()
              .getClientSequenceNumber())
          .build();

      guardedNodesDedups.write(dedupUnits -> {
        var dedupUnit = dedupUnits.get(entry.getRequestId()
            .getClientNodeIdentity());
        if (dedupUnit != null) {
          if (dedupUnit.lastResult()
              .getLastSeqNum() < newLastResult.getLastSeqNum()) {
            dedupUnits.put(entry.getRequestId()
                .getClientNodeIdentity(), new DedupUnit(newLastResult, null));
          } else {
            throw new IllegalStateException(
                "seq num of newLastResult must be greater than seq num of lastResult");
          }
        } else {
          dedupUnits.put(entry.getRequestId()
              .getClientNodeIdentity(), new DedupUnit(newLastResult, null));
        }
        return dedupUnits;
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

  public StateSnapshot takeSnapshot(long lastSnapshotEndApplyIndex, long leaderEpoch)
      throws InterruptedException {
    applyLock.lock();
    try {
      StateSnapshot.Builder snapshotBuilder = StateSnapshot.newBuilder();
      log.debug()
          .addKeyValue("applyIndex", applyIndex)
          .log("Taking snapshot");
      snapshotBuilder.setLeaderEpoch(leaderEpoch)
          .setStartApplyIndex(lastSnapshotEndApplyIndex + 1)
          .setEndApplyIndex(applyIndex);
      s2cStateMachineRegistry.getAll()
          .forEach(e -> {
            ByteString snapshotByteStr = e.snapshot();
            StateMachineSnapshot snapshot = StateMachineSnapshot.newBuilder()
                .setBody(snapshotByteStr)
                .setSmName(e.name())
                .build();
            snapshotBuilder.addSmSnapshots(snapshot);
          });

      guardedNodesDedups.write(dedupUnits -> {
        dedupUnits.applyOnIterator(it -> {
          while (it.hasNext()) {
            var dedupUnit = it.next()
                .getValue();
            snapshotBuilder.addLastResults(dedupUnit.lastResult());
          }
        });
        return dedupUnits;
      });

      log.debug()
          .addKeyValue("applyIndex", applyIndex)
          .log("Snapshot ready");
      return snapshotBuilder.build();
    }
    finally {
      applyLock.unlock();
    }
  }

  private long applySnapshot(StateSnapshot stateSnapshot) throws InterruptedException {

    guardedNodesDedups.write(dedupUnits -> {
      stateSnapshot.getLastResultsList()
          .forEach(lastResult -> {
            dedupUnits.put(lastResult.getNodeIdentity(), new DedupUnit(lastResult, null));
          });
      return dedupUnits;
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

  private ByteString applyInternal(ByteString body) throws ApplicationException {
    try {
      InternalStateRequest internalStateRequest = InternalStateRequest.parseFrom(body);

      return internalStateRequestHandler.handle(internalStateRequest);
    }
    catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("Invalid internal request, e");
    }
    catch (InterruptedException | S2CStoppedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread()
            .interrupt();
      }
      // Will be translated to InternalError the StateRequestHandler
      throw new ApplicationException("Error while handling internal state request", e);
    }
  }

  private void doApplyBatch(Collection<TraceableStateRequest> batch) {
    for (var t : batch) {
      try {
        ByteString result = apply(t.correlationId(),
            t.reqRes()
                .request()
                .getSourceSm(),
            t.reqRes()
                .request()
                .getBody(),
            t.reqRes()
                .request()
                .getType(),
            t.reqRes()
                .request()
                .getInternal());
        log.trace()
            .addKeyValue("correlationId", t.correlationId())
            .log("Request applied");
        t.reqRes()
            .response(result);
      }
      catch (ApplicationException e) {
        log.debug()
            .setCause(e)
            .addKeyValue("correlationId", t.correlationId())
            .addKeyValue("internal",
                t.reqRes()
                    .request()
                    .getInternal())
            .log("Error while applying request");
        t.reqRes()
            .exception(e);
      }

      if (t.reqRes()
          .request()
          .getType() == StateRequestType.COMMAND) {
        LastResult.Builder lastResultBuilder = LastResult.newBuilder();

        lastResultBuilder.setNodeIdentity(t.reqRes()
            .request()
            .getSourceNode())
            .setLastSeqNum(t.reqRes()
                .request()
                .getSequenceNumber());
        if (t.reqRes()
            .excption() != null) {
          lastResultBuilder.setErrMsg(t.reqRes()
              .excption()
              .getMessage());
          lastResultBuilder.setResult(ByteString.EMPTY);
        } else {
          lastResultBuilder.setResult(t.reqRes()
              .response());
          lastResultBuilder.setErrMsg(NO_ERR_MSG);
        }

        LastResult newLastResult = lastResultBuilder.build();

        guardedNodesDedups.write(dedupUnits -> {
          DedupUnit dedpUnit = dedupUnits.get(t.reqRes()
              .request()
              .getSourceNode());
          if (dedpUnit != null) {
            if (dedpUnit.lastResult()
                .getLastSeqNum() < newLastResult.getLastSeqNum()) {
              dedupUnits.put(t.reqRes()
                  .request()
                  .getSourceNode(), new DedupUnit(newLastResult, dedpUnit.eosBuffer()));
            }
          } else {
            dedupUnits.put(t.reqRes()
                .request()
                .getSourceNode(), new DedupUnit(newLastResult, null));
          }
          return dedupUnits;
        });
      }
    }
  }

  private ByteString apply(String correlationId, String stateMachineName, ByteString requestBody,
      StateRequestType stateRequestType, boolean internal) throws ApplicationException {
    S2CStateMachine stateMachine = s2cStateMachineRegistry.get(stateMachineName);
    if (stateMachine == null) {
      // This should never happen, as state machine creation must be made
      // by createOrRegisterStateMachine, otherwise it will throw by calling
      // apply() on the state machine object itself.
      throw new IllegalStateException("Unknown stateMachineName %s".formatted(stateMachineName));
    }
    if (internal) {
      log.trace()
          .addKeyValue("correlationId", correlationId)
          .log("Applying internal state request");
      return applyInternal(requestBody);
    } else {
      log.trace()
          .addKeyValue("correlationId", correlationId)
          .addKeyValue("stateMachineName", stateMachineName)
          .log("Delegating request to state machine");

      return stateMachine.handleRequest(requestBody, stateRequestType);
    }

  }

  private Optional<StateSnapshot> restoreSnapshot() throws InterruptedException {
    log.trace()
        .log("Restoring snapshot");
    Optional<StateSnapshot> stateSnapshotOptional = stateSnapshotSupplier.get();
    if (stateSnapshotOptional.isEmpty()) {
      log.debug()
          .log("No valid snapshot.");
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
    log.debug()
        .addKeyValue("applyIndex", applyIndex)
        .log("Snapshot applied.");
    return stateSnapshotOptional;
  }
}