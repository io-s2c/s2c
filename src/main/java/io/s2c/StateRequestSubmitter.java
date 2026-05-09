package io.s2c;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.s2c.configs.S2COptions;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.NotLeaderError;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.StateRequest;
import io.s2c.model.state.LeaderState;
import io.s2c.network.S2CClient;
import io.s2c.network.error.ClientNotConnectedException;
import io.s2c.network.error.ClientStoppedException;
import io.s2c.util.BackoffCounter;

public class StateRequestSubmitter implements AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(StateRequestSubmitter.class);
  private final StructuredLogger log;
  private final NodeStateManager nodeStateManager;
  private final LeaderStateManager leaderStateManager;
  private final BiFunction<String, StateRequest, S2CMessage> localHandler;
  private final S2CClient s2cClient;
  private final S2COptions s2cOptions;
  private final ContextProvider contextProvider;

  private final AtomicBoolean resetting = new AtomicBoolean();

  private volatile boolean closed = false;
  private volatile long lowestSeqNum = Long.MAX_VALUE;
  private Timer submitLatency;

  public StateRequestSubmitter(NodeStateManager nodeStateManager,
      LeaderStateManager leaderStateManager,
      BiFunction<String, StateRequest, S2CMessage> localHandler,
      S2CClient s2cClient,
      ContextProvider contextProvider,
      S2COptions s2cOptions,
      MeterRegistry meterRegistry) {
    this.nodeStateManager = nodeStateManager;
    this.leaderStateManager = leaderStateManager;
    this.localHandler = localHandler;
    this.s2cClient = s2cClient;
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.s2cOptions = s2cOptions;
    this.contextProvider = contextProvider;

    submitLatency = Timer.builder("state.request.submit.latency")
        .description("The end to end latency of a submitted request handled by the leader")
        .register(meterRegistry);

  }

  public S2CMessage submit(StateRequest stateRequest)
      throws InterruptedException, S2CStoppedException, ClientStoppedException {

    if (stateRequest.getSequenceNumber() < lowestSeqNum) {
      lowestSeqNum = stateRequest.getSequenceNumber();
    }

    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cOptions.s2cRetryOptions())
        .build();

    S2CMessage s2cMessage = S2CMessage.newBuilder()
        .setCorrelationId(UUID.randomUUID()
            .toString())
        .setStateRequest(stateRequest)
        .build();

    while (true) {
      if (closed) {
        throw new S2CStoppedException();
      }
      nodeStateManager.awaitJoined();
      LeaderState leaderState = leaderStateManager.getLeaderState();
      try {
        S2CMessage response = send(s2cMessage, leaderState);
        if (response.hasRequestOutOfSequenceError()) {
          long backOff = Math.max(0,
              stateRequest.getSequenceNumber() - response.getRequestOutOfSequenceError()
                  .getNextSeqNum());
          TimeUnit.MILLISECONDS.sleep(backOff);
        } else if (response.hasNotLeaderError()) {
          if (stateRequest.getLeaderCommand()) {
            return response;
          }
          log.debug()
              .log("Leader responded with NotLeaderError. Checking leader state then retrying");
          // Check then await above NodeStateManager to connect
          leaderStateManager.checkLeaderState(leaderState);
        } else if (response.hasInternalError()) {
          log.debug()
              .log("Leader responded with internal error.");
          backoffCounter.awaitNextAttempt();
        } else if (response.hasSlowDownError()) {
          log.debug()
              .log("Leader responded with slow down error");
          backoffCounter.awaitNextAttempt();
        } else if (response.hasLeaderStartingError()) {
          log.debug()
              .log("Leader responded with leader starting error");
          backoffCounter.awaitNextAttempt();
        } else if (response.hasNotFollowerError()) { // This can happen when the same leader node
                                                     // has restarted
          log.debug()
              .log("Leader responded with not follower error, resetting leader state");
          if (resetting.compareAndSet(false, true)) {
            leaderStateManager.resetLeaderState();
            resetting.set(false);
          } else {
            leaderStateManager.getLeaderState(); // Block while other thread is resetting
          }
        } else {
          backoffCounter.reset();
          return response;
        }
      }
      // Connection to the leader was lost and IOException was handled by S2CClient
      catch (ClientNotConnectedException | IOException e) {
        log.debug()
            .setCause(e)
            .log("Error while sending.");
        if (resetting.compareAndSet(false, true)) {
          leaderStateManager.resetLeaderState();
          resetting.set(false);
        } else {
          leaderStateManager.getLeaderState(); // Block while other thread is resetting
        }
      }
      catch (TimeoutException e) {
        log.debug()
            .setCause(e)
            .log("Timed out.");
        // Timeout might also mean the leader is active but busy, that's why we don't trigger
        // leadership check
        // and leave this to the LeaderHealthMonitor which will trigger a leadership reset if leader
        // stopped sending heartbeats
        if (!backoffCounter.canAttempt()) {
          // Trigger reconnect by NodeStateManager
          if (resetting.compareAndSet(false, true)) {
            leaderStateManager.resetLeaderState();
            nodeStateManager.awaitJoined();
            resetting.set(false);
          } else {
            leaderStateManager.getLeaderState(); // Block while other thread is resetting
          }
          backoffCounter.reset();
        } else {
          backoffCounter.enrich(log.debug())
              .log("Retrying...");
          backoffCounter.awaitNextAttempt();
        }
      }
    }
  }

  @Override
  public void close() {
    closed = true;
  }

  private S2CMessage send(S2CMessage s2cMessage, LeaderState leaderState)
      throws InterruptedException, IOException, ClientStoppedException, TimeoutException,
      ClientNotConnectedException {
    S2CMessage response = null;
    var logBuilder = log.debug()
        .addKeyValue("correlationId", s2cMessage.getCorrelationId());
    if (leaderStateManager.isLeader(leaderState)) {
      // Handle request on the local state machine
      response = localHandler.apply(s2cMessage.getCorrelationId(), s2cMessage.getStateRequest());
    } else {
      if (s2cMessage.getStateRequest()
          .getLeaderCommand()) {
        return S2CMessage.newBuilder()
            .setNotLeaderError(NotLeaderError.getDefaultInstance())
            .build();
      }
      Timer.Sample sample = Timer.start();
      response = s2cClient.send(s2cMessage, s2cOptions.requestTimeoutMs());
      sample.stop(submitLatency);
      logBuilder.addKeyValue("response", response)
          .log("Response received");
    }
    return response;
  }
}
