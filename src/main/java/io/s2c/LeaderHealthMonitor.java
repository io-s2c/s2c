package io.s2c;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.s2c.concurrency.Awaiter;
import io.s2c.concurrency.Sequencer;
import io.s2c.concurrency.Task;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.LeaderState;

public class LeaderHealthMonitor implements Task {

  private final Heartbeat POISON_PILL = new Heartbeat();

  private final Logger logger = LoggerFactory.getLogger(LeaderHealthMonitor.class);
  private final StructuredLogger log;

  private final SynchronousQueue<Heartbeat> heartbeats = new SynchronousQueue<>();

  private final LeaderStateManager leaderStateManager;
  private final Awaiter<LeaderState, S2CStoppedException> leaderStateAwaiter;
  private final int maxMissedHeartbeats;
  private final int heartbeatTimeoutMs;

  private volatile boolean running;

  private int missedHeartbeats = 0;

  public LeaderHealthMonitor(LeaderStateManager leaderStateManager,
      int maxMissedHeartbeats,
      int heartbeatTimeoutMs,
      ContextProvider contextProvider) {
    this.leaderStateManager = leaderStateManager;
    this.maxMissedHeartbeats = maxMissedHeartbeats;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.leaderStateAwaiter = leaderStateManager.getNewAwaiter();
  }

  @Override
  public void init() {
    running = true;
  }

  @Override
  public void run() {
    while (running) {
      try {
        leaderStateAwaiter.await(ls -> !leaderStateManager.isLeader(ls));
        missedHeartbeats = 0;
        while (running) {
          var heartbeat = heartbeats.poll(heartbeatTimeoutMs, TimeUnit.MILLISECONDS);
          if (heartbeat == POISON_PILL) {
            return;
          }
          if (heartbeat == null) {
            missedHeartbeats++;
            log.debug()
                .addKeyValue("missedHeartbeats", missedHeartbeats)
                .log("Leader heartbeat timed out.");
            if (missedHeartbeats >= maxMissedHeartbeats && running) {
              // Catch up leader state only, and re-follow
              leaderStateManager.resetLeaderState();
              missedHeartbeats = 0;
            }
          } else {
            int missed = missedHeartbeats;
            missedHeartbeats = 0;
            if (missed > 0) {
              log.debug().addKeyValue("missedHeartbeats", missed).log("Missed heartbeats reset");
            }
          }
          LeaderState leaderState = leaderStateManager.getLeaderState();
          if (leaderStateManager.isLeader(leaderState)) {
            break;
          }
        }
      } catch (InterruptedException | S2CStoppedException e) {
          log.debug().setCause(e).log("Error while monitoring leader's health");
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        stopQuietly();
      }
    }

  }

  public void registerHeartbeat(Heartbeat heartbeat) {
    heartbeats.offer(heartbeat);
  }

  @Override
  public void close() throws InterruptedException {
    running = false;
  }

  private void stopQuietly() {
    try {
      close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

}
