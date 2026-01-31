package io.s2c;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.s2c.configs.S2COptions;
import io.s2c.error.ConnectTimeoutException;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.StateRequest;
import io.s2c.model.state.LeaderState;
import io.s2c.network.S2CClient;
import io.s2c.network.error.ClientNotConnectedException;
import io.s2c.network.error.ClientStoppedException;
import io.s2c.network.error.UnknownHostException;
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
        .setCorrelationId(UUID.randomUUID().toString())
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
          long backOff = Math.max(0, stateRequest.getSequenceNumber()
              - response.getRequestOutOfSequenceError().getNextSeqNum());
          TimeUnit.MILLISECONDS.sleep(backOff);

          continue;
        } else if (response.hasNotLeaderError()) {
          log.debug()
              .log("Leader responded with NotLeaderError. Checking leader state then retrying");
          leaderState = leaderStateManager.checkLeaderState(leaderState);
          s2cClient.disconnect();
          backoffCounter.reset();
          continue;
        } else if (response.hasInternalError()) {
          log.debug().log("Leader responded with internal error.");
        } else if (response.hasSlowDownError()) {
          log.debug().log("Leader responded with slow down error");
        } else if (response.hasLeaderStartingError()) {
          log.debug().log("Leader responded with leader starting error");
        } else {
          backoffCounter.reset();
          return response;
        }
      }
      catch (IOException | ClientNotConnectedException e) {
        log.debug().setCause(e).log("Error while sending.");
        log.debug().log("Reconnecting...");
        if (reconnect(leaderState)) {
          log.debug().log("Reconnected successfully");
          backoffCounter.reset();
          continue;
        } else {
          leaderState = leaderStateManager.checkLeaderState(leaderState);
          // Fail pending requests - reconnect on next iteration
          s2cClient.disconnect();
          backoffCounter.reset();
          continue;
        }
      }
      catch (TimeoutException e) {
        log.debug()
            .setCause(e)
            .addKeyValue("leaderNodeIdentity", leaderState.getNodeIdentity())
            .log("Request timed out.");
      }
      // If timed-out or no successful response
      if (backoffCounter.canAttempt()) {
        backoffCounter.enrich(log.debug()).log("Retrying..");
        backoffCounter.awaitNextAttempt();
      } else {
        leaderState = leaderStateManager.checkLeaderState(leaderState);
        // Fail other threads
        s2cClient.disconnect();
        if (!leaderStateManager.isLeader(leaderState)) {
          reconnect(leaderState);
        }
        backoffCounter.reset();
      }
    }
  }

  private boolean reconnect(LeaderState leaderState) throws InterruptedException {
    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cOptions.s2cRetryOptions())
        .build();
    try {
      s2cClient.disconnect();
      s2cClient.connect(leaderState.getNodeIdentity());
      return true;
    }
    catch (ConnectTimeoutException | UnknownHostException | IOException
        | ClientNotConnectedException e) {
      log.debug().setCause(e).log("Error while reconnecting to leader");
    }
    if (backoffCounter.canAttempt()) {
      backoffCounter.enrich(log.debug()).log("Retrying");
      backoffCounter.awaitNextAttempt();
    }
    return false;
  }

  @Override
  public void close() {
    closed = true;
  }

  private S2CMessage send(S2CMessage s2cMessage, LeaderState leaderState)
      throws InterruptedException, IOException, ClientStoppedException, TimeoutException,
      ClientNotConnectedException {
    S2CMessage response = null;
    var logBuilder = log.debug().addKeyValue("correlationId", s2cMessage.getCorrelationId());
    if (leaderStateManager.isLeader(leaderState)) {
      // Handle request on the local state machine
      response = localHandler.apply(s2cMessage.getCorrelationId(), s2cMessage.getStateRequest());
    } else {
      Timer.Sample sample = Timer.start();

      response = s2cClient.send(s2cMessage, s2cOptions.requestTimeoutMs());

      sample.stop(submitLatency);
      logBuilder.addKeyValue("response", response).log("Response received");
    }
    return response;
  }
}
