package io.s2c;

import static io.s2c.network.S2CGroupServer.POISON_PILL;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.s2c.concurrency.Task;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.SlowDownError;

public class ClientMessageAcceptor implements AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(ClientMessageAcceptor.class);

  private final StructuredLogger log;
  private final BlockingQueue<S2CMessage> out;
  private final ClientMessageHandler clientMessageHandler;

  private final TaskExecutor taskExecutor;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean handshaked = new AtomicBoolean();
  
  private final MeterRegistry meterRegistry;
  private Counter slowdownResponses;
  private Timer enqueueBackpressure;

  ClientMessageAcceptor(ClientMessageHandler clientMessageHandler,
      ContextProvider contextProvider,
      int maxConcurreny,
      int maxPendingResponses,
      MeterRegistry meterRegistry) {
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.taskExecutor = new TaskExecutor(contextProvider.ownerName(ClientMessageAcceptor.class),
        log.uncaughtExceptionLogger(), 200, meterRegistry);
    this.clientMessageHandler = clientMessageHandler;
    this.meterRegistry = meterRegistry;
    this.out = new LinkedBlockingQueue<>(maxPendingResponses);
  }

  public void acceptIn(S2CMessage s2cMessage) throws InterruptedException {
    
    if (s2cMessage.hasHandshake()) {
      if (handshaked.compareAndSet(false, true)) {
        String clientNodeIdentityStr = s2cMessage.getHandshake().getNodeIdentity().toString();
        initMetrics(clientNodeIdentityStr);
        out.put(s2cMessage); // echo
      } else {
        log.warn().log("Rejected an attempt to handshake as the client was already handshaked");
      }
    }
    // Follower if synchronize -> no concurrency.
    if (s2cMessage.hasSynchronizeRequest()) {
      routeAndAwait(s2cMessage);
    } else {
      route(s2cMessage);
    }
  }

  private void route(S2CMessage message) throws InterruptedException {
    boolean shouldThrottle = !taskExecutor.tryStart(Task.of(() -> routeAndAwait(message)));
    if (shouldThrottle) {
      slowdownResponses.increment();
      log.debug()
      .addKeyValue("correlationId", message.getCorrelationId())
      .addKeyValue("maxConcurrency", taskExecutor.maxConcurrency())
      .addKeyValue("currentTasksCount", taskExecutor.currentTasksCount())
      .log("Message handlig task rejected by task executor.");
      S2CMessage s2cMessage = S2CMessage.newBuilder()
          .setCorrelationId(message.getCorrelationId())
          .setSlowDownError(SlowDownError.getDefaultInstance())
          .build();
      Timer.Sample sample = Timer.start();
      out.put(s2cMessage);
      sample.stop(enqueueBackpressure);
    }
  }

  private void routeAndAwait(S2CMessage s2cMessage) {
    try {
      S2CMessage res = null;
      log.trace()
      .addKeyValue("message", s2cMessage)
      .log("Handling message.");
      if (s2cMessage.hasSynchronizeRequest()) {
        res = clientMessageHandler.handleSynchronize(s2cMessage.getCorrelationId(),
            s2cMessage.getSynchronizeRequest());
      } else if (s2cMessage.hasStateRequest()) {
        res = clientMessageHandler.handleStateRequest(s2cMessage.getCorrelationId(),
            s2cMessage.getStateRequest());
      } else if (s2cMessage.hasFollow()) {
        res = clientMessageHandler.handleFollow(s2cMessage.getCorrelationId(),
            s2cMessage.getFollow());
      }

      if (res != null) {
        Timer.Sample sample = Timer.start();
        out.put(res);
        sample.stop(enqueueBackpressure);
      }

    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error()
      .addKeyValue("correlationId", s2cMessage.getCorrelationId())
      .log("Interrupted while handling", e);
    }
  }

  public S2CMessage nextResponse() throws InterruptedException {
    return out.take();
  }

  @Override
  public void close() throws InterruptedException {
    if (closed.compareAndSet(false, true)) {
      out.put(POISON_PILL);
      taskExecutor.close();
    }
  }

  private void initMetrics(String clientNodeIdentityStr) {
    Gauge.builder("queued.responses", out::size)
    .tag("clientNodeIdentity", clientNodeIdentityStr)
    .register(meterRegistry);

    slowdownResponses = Counter.builder("outgoing.slowdown.responses")
        .description("The number of slow down responses because of max concurrency limit reached")
        .tag("clientNodeIdentity", clientNodeIdentityStr)
        .register(meterRegistry);

    enqueueBackpressure = Timer.builder("responses.enqueue.backpressure")
        .description(
            "The time that was spend waiting for the responses queue to accept the response")
        .tag("clientNodeIdentity", clientNodeIdentityStr)
        .register(meterRegistry);
  }
  
}