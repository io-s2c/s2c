package io.s2c;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.s2c.concurrency.Task;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.configs.S2COptions;
import io.s2c.error.ConcurrentStateModificationException;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.StateSnapshot;
import io.s2c.util.ConcurrentStateModificationExceptionHandler;

public class SnapshottingWorker implements Task {

  private final Logger logger = LoggerFactory.getLogger(SnapshottingWorker.class);
  private final StructuredLogger log;

  private final LeaderStateManager leaderStateManager;
  private final SnapshotStorageManager stateSnapshotManager;
  private final RSM rsm;
  private final ConcurrentStateModificationExceptionHandler exHandler;
  private final TaskExecutor singleTaskExecutor;
  private final BiConsumer<Long, Long> snapshotUploadHandler;
  private final S2COptions s2cOptions;
  private final AtomicInteger notificationCounter = new AtomicInteger();
  private volatile boolean running = false;

  public SnapshottingWorker(LeaderStateManager leaderStateManager,
      ContextProvider contextProvider,
      SnapshotStorageManager stateSnapshotManager,
      RSM rsm,
      ConcurrentStateModificationExceptionHandler exHandler,
      BiConsumer<Long, Long> snapshotUploadHandler,
      S2COptions s2cOptions,
      MeterRegistry meterRegistry) {
    this.leaderStateManager = leaderStateManager;
    this.rsm = rsm;
    this.s2cOptions = s2cOptions;
    this.exHandler = exHandler;
    this.stateSnapshotManager = stateSnapshotManager;
    this.snapshotUploadHandler = snapshotUploadHandler;
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.singleTaskExecutor = new TaskExecutor(contextProvider.ownerName(SnapshottingWorker.class),
        log.uncaughtExceptionLogger(), 1, meterRegistry);
  }

  public void init() {
    running = true;
  }

  public void run() {
    while (running) {
      try {
        rsm.awaitApply();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e).log("Interrupted");
        close();
      }
      int count = notificationCounter.incrementAndGet();
      if (count >= s2cOptions.snapshottingThreshold()) {
        singleTaskExecutor.tryStart("snapshot-upload", Task.of(this::upload));
        notificationCounter.set(0);
      }
    }
  }

  private void upload() {
    try {
      LeaderState leaderState = leaderStateManager.getLeaderState();
      long lastSnapshotApplyIndex = stateSnapshotManager.currentSnapshotApplyIndex();
      StateSnapshot snapshot = rsm.takeSnapshot(lastSnapshotApplyIndex, leaderState.getEpoch());
      try {
        stateSnapshotManager.upload(snapshot, leaderState);
        // lastSnapshotApplyIndex must already be deleted
        snapshotUploadHandler.accept(lastSnapshotApplyIndex + 1,
            stateSnapshotManager.currentSnapshotApplyIndex());
        log.debug().log("Snapshot uploaded");
      }
      catch (ConcurrentStateModificationException e) {
        log.debug().setCause(e).log("Snapshot upload failed");
        exHandler.accept(e);
      }
    }
    catch (S2CStoppedException | InterruptedException e) {
      log.debug().setCause(e).log("Error while uploading snapshot");
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      close();
    }

  }

  public void close() {
    running = false;
    singleTaskExecutor.close();
  }

}
