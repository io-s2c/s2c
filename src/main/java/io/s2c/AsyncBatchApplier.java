package io.s2c;

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.s2c.concurrency.Task;
import io.s2c.configs.S2COptions;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.LogEntriesBatch;

public class AsyncBatchApplier implements Task {

  private final LogEntriesBatch POISON_PILL = LogEntriesBatch.newBuilder().build();

  private final Logger logger = LoggerFactory.getLogger(AsyncBatchApplier.class);
  private final StructuredLogger log;

  private final LinkedBlockingQueue<LogEntriesBatch> pendingBatches;

  private final RSM rsm;

  private volatile boolean running = false;

  private long lastBatchCommitIndex = 0;

  public AsyncBatchApplier(RSM rsm, ContextProvider contextProvider, S2COptions s2cOptions) {
    this.rsm = rsm;
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.pendingBatches = new LinkedBlockingQueue<>(s2cOptions.maxBatchesPendingForApply());
  }

  @Override
  public void run() {
    while (running) {
      try {
        var batch = pendingBatches.take();
        boolean accepted = rsm.applyLogEntriesBatchQuietly(batch);
        if (!accepted) {
          log.debug()
              .log("RSM rejected applying a committed batch from a SynchronizeRequest. "
                  + "This indicates a node's state transition. Clearing pending requests");
          pendingBatches.clear();
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
          log.debug().setCause(e).log("Interrupted");
      }
    }
  }

  public long apply(LogEntriesBatch logEntriesBatch) {
    boolean added = pendingBatches.offer(logEntriesBatch);
    if (added) {
      lastBatchCommitIndex = logEntriesBatch.getCommitIndex();
    }
    return lastBatchCommitIndex;
  }

  @Override
  public void close() throws InterruptedException {
    if (running) {
      pendingBatches.put(POISON_PILL);
      running = false;
    }
  }

  @Override
  public void init() {
    running = true;
  }

}
