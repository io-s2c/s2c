package io.s2c;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.s2c.concurrency.Awaiter;
import io.s2c.concurrency.Task;
import io.s2c.configs.S2COptions;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.LogEntriesBatch;

public class AsyncBatchApplier implements Task {

  private static final LogEntriesBatch POISON_PILL = LogEntriesBatch.newBuilder()
      .build();

  private static final LogEntriesBatch CLEAR = LogEntriesBatch.newBuilder()
      .build();

  private final Logger logger = LoggerFactory.getLogger(AsyncBatchApplier.class);
  private final StructuredLogger log;

  private final LeaderStateManager leaderStateManager;
  private final Awaiter<LeaderState, S2CStoppedException> awaiter;
  private final LinkedBlockingQueue<LogEntriesBatch> pendingBatches;

  private final RSM rsm;

  private final SynchronousQueue<Boolean> clearSynchronousQueue = new SynchronousQueue<>();
  private volatile boolean running = false;

  private long lastBatchCommitIndex = 0;

  public AsyncBatchApplier(RSM rsm,
      LeaderStateManager leaderStateManager,
      ContextProvider contextProvider,
      S2COptions s2cOptions) {
    this.rsm = rsm;
    this.leaderStateManager = leaderStateManager;
    this.awaiter = leaderStateManager.getNewAwaiter();
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.pendingBatches = new LinkedBlockingQueue<>(s2cOptions.maxBatchesPendingForApply());
  }

  @Override
  public void run() {
    while (true) {
      try {
        awaiter.await(l -> !leaderStateManager.isLeader(l));
        running = true;
        while (running) {
          var batch = pendingBatches.take();
          if (batch == POISON_PILL) {
            running = false;
            return;
          }
          // When node is leader
          if (batch == CLEAR) {
            // Clear CLEAR and any new batches
            pendingBatches.clear();
            clearSynchronousQueue.put(false);
            running = false;
            continue;
          }
          rsm.applyLogEntriesBatchQuietly(batch);
          pendingBatches.clear();
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread()
            .interrupt();
        log.debug()
            .setCause(e)
            .log("Interrupted");
        break;
      }
      catch (S2CStoppedException e) {
        log.debug()
            .setCause(e)
            .log("Error in loop");
        break;
      }
    }
  }

  public long apply(LogEntriesBatch logEntriesBatch) {
    try {
      if (!leaderStateManager.isLeader(leaderStateManager.getLeaderState())) {
        boolean added = pendingBatches.offer(logEntriesBatch);
        if (added) {
          lastBatchCommitIndex = logEntriesBatch.getCommitIndex();
        }
      }
    }
    catch (InterruptedException | S2CStoppedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread()
            .interrupt();
      }
      log.debug()
          .setCause(e)
          .log("Error while appending batch");
      close();
    }
    return lastBatchCommitIndex;
  }

  public void clear() throws InterruptedException {
    if (running) {
      pendingBatches.clear();
      pendingBatches.put(CLEAR);
      clearSynchronousQueue.take();
    }
  }

  @Override
  public void close() {
    pendingBatches.clear();
    pendingBatches.offer(POISON_PILL);
  }

  @Override
  public void init() {}

}
