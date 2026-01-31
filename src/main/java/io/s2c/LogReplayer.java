package io.s2c;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import io.s2c.concurrency.Task;
import io.s2c.model.state.LogEntriesBatch;

class LogReplayer implements Task {

  private final LogEntriesBatch POISON_PILL = LogEntriesBatch.newBuilder().build();

  private final Function<Long, LogEntriesBatch> batchDownloader;

  static class ReplayerBrokenException extends Exception {
    public ReplayerBrokenException(Throwable cause) {
      super(cause);
    }

    public ReplayerBrokenException(String message) {
      super(message);
    }
  }

  private final long from;
  private final long to;
  private final long maxWaiting;
  private final LinkedBlockingQueue<LogEntriesBatch> downloaded;
  private ReplayerBrokenException replayerBrokenException;
  private long lastConsumed = 0;
  private long lastDownloaded = 0;
  private volatile boolean running = false;

  LogReplayer(long from, long to, Function<Long, LogEntriesBatch> batchDownloader) {
    this.from = from;
    this.to = to;
    maxWaiting = (to - from) / 2;
    long bound = maxWaiting == 0 ? Integer.MAX_VALUE : maxWaiting;
    downloaded = new LinkedBlockingQueue<>((int) bound);
    this.batchDownloader = batchDownloader;
  }

  @Override
  public void init() {
    running = true;
  }

  @Override
  public void close() throws InterruptedException {
    running = false;
    downloaded.put(POISON_PILL);
  }

  @Override
  public void run() {
    lastDownloaded = from;
    while (lastDownloaded <= to && running) {
      try {
        LogEntriesBatch batch = batchDownloader.apply(lastDownloaded);
        if (batch == null) {
          close();
          running = false;
          break;
        }
        // Still running after download?
        if (running) {
          downloaded.put(batch);
          lastDownloaded++;
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        replayerBrokenException = new ReplayerBrokenException(e);
        break;
      }
    }
  }

  public synchronized LogEntriesBatch next() throws InterruptedException, ReplayerBrokenException {
    if (lastConsumed > (to - from) || !running) {
      return null;
    }
    if (replayerBrokenException != null) {
      throw replayerBrokenException;
    }
    var batch = downloaded.take();
    if (batch == POISON_PILL) {
      return null;
    }
    lastConsumed++;
    return batch;
  }

  public long downloadIndex() {
    return lastDownloaded;
  }

  public long consumeIndex() {
    return lastConsumed;
  }
  
  public long currentCommitIndex() {
    return lastConsumed + from; // from is the start of the log replayer
  }
}