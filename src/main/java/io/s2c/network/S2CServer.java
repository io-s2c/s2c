package io.s2c.network;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.s2c.concurrency.Task;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.configs.S2CNetworkOptions;
import io.s2c.configs.S2COptions;
import io.s2c.configs.S2CRetryOptions;
import io.s2c.error.MessageTooLargeException;
import io.s2c.logging.LoggingContext;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.error.FatalServerException;
import io.s2c.network.error.InvalidHandshakeMessage;
import io.s2c.network.message.reader.S2CMessageReader;
import io.s2c.util.BackoffCounter;

public class S2CServer implements AutoCloseable {

  // NOT_BOUND -> BINDING -> READY
  // READY -> RESTARTING
  // REASTARTING -> READY
  // * -> CLOSED

  enum ServerState {
    NOT_BOUND, BINDING, READY, RESTARTING, CLOSED
  }

  private final Logger logger = LoggerFactory.getLogger(S2CServer.class);
  private final StructuredLogger log;

  private final Map<String, S2CGroupServer> s2cGroupServers = new ConcurrentHashMap<>();

  private final NodeIdentity nodeIdentity;
  private final TaskExecutor taskExecutor;
  private final S2COptions s2cOptions;
  private final S2CMessageReader s2cMessageReader;
  private ServerSocket serverSocket;

  private AtomicReference<ServerState> state = new AtomicReference<>(ServerState.NOT_BOUND);
  private final CountDownLatch joinLatch = new CountDownLatch(1);
  private Counter ioErrorCounter;
  private Counter invalidHandshakesCounter;
  private Counter clientsWithUnknownGroupCounter;
  private Counter serverRestartsCounter;
  private Timer handshakeTimer;

  public S2CServer(NodeIdentity nodeIdentity, S2COptions s2cOptions) {
    this(nodeIdentity,
        () -> S2CMessageReader.create(s2cOptions.maxMessageSize()),
        s2cOptions,
        Metrics.globalRegistry);
  }

  public S2CServer(NodeIdentity nodeIdentity, S2COptions s2cOptions, MeterRegistry meterRegistry) {
    this(nodeIdentity,
        () -> S2CMessageReader.create(s2cOptions.maxMessageSize()),
        s2cOptions,
        meterRegistry);
  }

  public S2CServer(NodeIdentity nodeIdentity,
      Supplier<S2CMessageReader> s2cMessageReadyFactory,
      S2COptions s2cOptions,
      MeterRegistry meterRegistry) {

    this.nodeIdentity = nodeIdentity;

    this.s2cMessageReader = s2cMessageReadyFactory.get();

    // S2CServer has no ContextProvider as it is group-independent
    this.log = new StructuredLogger(logger, LoggingContext.builder().build());

    this.taskExecutor = new TaskExecutor("S2CServer", log.uncaughtExceptionLogger(), meterRegistry);
    this.s2cOptions = s2cOptions;

    initMetrics(meterRegistry);

  }

  public void start() throws IOException {
    if (state.compareAndSet(ServerState.NOT_BOUND, ServerState.BINDING)) {
      log.info().log("Starting S2CServer");
      bind();
      taskExecutor.start("client-acceptor", Task.of(this::accept, this::closeServerSocket));
    } else {
      log.warn().log("Cannot start S2CServer as it is already running");
    }

  }

  private void bind() throws IOException {
    try {
      if (state.compareAndSet(ServerState.BINDING, ServerState.READY)) {
        serverSocket = new ServerSocket(nodeIdentity.getPort());
        log.debug().log("Server bound and ready");
      } else {
        log.warn().log("Unexpected state for binding");
      }
    }
    catch (IOException e) {
      log.debug().setCause(e).log("Error while binding");
      ioErrorCounter.increment();
      closeServerSocket();
      throw e;
    }
  }

  private void accept() {
    while (state.get() == ServerState.READY) {
      try {
        try {
          Socket socket = serverSocket.accept();
          taskExecutor.start(Task.of(() -> handleClient(socket), () -> closeSocket(socket)));
        }
        catch (IOException e) {
          ioErrorCounter.increment();
          log.debug().setCause(e).log("Error while accepting connection");
          if (Thread.currentThread().isInterrupted()) {
            propagate(new FatalServerException(e));
            log.debug().log("Socket closed by interrupt");
            return;
          } else {
            restart();

          }
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e).log("Interrupted");
        propagate(new FatalServerException(e));
        break;
      }
    }
    log.debug().log("Socket acceptor ended.");
  }

  private void restart() throws InterruptedException {
    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cOptions.s2cRetryOptions())
        .unlimited()
        .build();
    if (!state.compareAndSet(ServerState.READY, ServerState.RESTARTING)) {
      log.debug().log("Ignoring restart as server is closed");
      return;
    }
    log.debug().log("Restarting server");
    closeServerSocket();
    while (backoffCounter.canAttempt() && state.get() == ServerState.RESTARTING) {
      try {
        bind();
        if (state.compareAndSet(ServerState.RESTARTING, ServerState.READY)) {
          log.debug().log("Server restarted");
          serverRestartsCounter.increment();
        } else {
          log.warn().log("Unexpected statef for restarting");
        }
        break;

      }
      catch (IOException e) {
        ioErrorCounter.increment();
        log.debug().setCause(e).log("Error while restarting server");
      }
      backoffCounter.enrich(log.debug()).log("Retrying");
      backoffCounter.awaitNextAttempt();
    }
  }

  private void handleClient(Socket socket) {
    try {
      Timer.Sample sample = Timer.start();
      log.debug().log("New socket accepted");
      socket.setSoTimeout(s2cOptions.s2cNetworkOptions().handshakeTimeout());
      log.debug()
          .addKeyValue("timeout", s2cOptions.s2cNetworkOptions().handshakeTimeout())
          .log("Awaiting handshake");
      S2CMessage handshakeMessage = awaitHandshake(socket.getInputStream());
      sample.stop(handshakeTimer);
      log.debug()
          .addKeyValue("correlationId", handshakeMessage.getCorrelationId())
          .log("Handshake succeeded");
      if (s2cGroupServers.containsKey(handshakeMessage.getHandshake().getGroupId())) {
        s2cGroupServers.get(handshakeMessage.getHandshake().getGroupId())
            .acceptClient(handshakeMessage.getCorrelationId(), handshakeMessage.getHandshake(),
                socket);
        log.debug()
            .addKeyValue("correlationId", handshakeMessage.getCorrelationId())
            .log("Client accepted");
      } else {
        clientsWithUnknownGroupCounter.increment();
        log.warn()
            .addKeyValue("groupId", handshakeMessage.getHandshake().getGroupId())
            .log("Ignoring client as groupId is unknown");
      }
    }
    catch (IOException | InvalidHandshakeMessage
        | InterruptedException e) {
      log.debug()
          .setCause(e)
          .addKeyValue("socketAddress", () -> socket.getInetAddress().toString())
          .log("Error while handling client");
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      if (e instanceof InvalidHandshakeMessage) {
        invalidHandshakesCounter.increment();
      }

      closeSocket(socket);
    } catch (MessageTooLargeException e) {
      throw new IllegalStateException(e);
    }
  }

  private void closeSocket(Socket socket) {
    if (socket != null && !socket.isClosed()) {
      try {
        socket.close();
      }
      catch (IOException e) {
        ioErrorCounter.increment();
        log.debug().setCause(e).log("Error while closing socket");
      }
    }
  }

  private S2CMessage awaitHandshake(InputStream in)
      throws IOException, MessageTooLargeException, InvalidHandshakeMessage {
    DataInputStream din = new DataInputStream(in);
    S2CMessage msg = s2cMessageReader.readNextMessage(din);
    if (msg.hasHandshake()) {
      return msg;
    } else {
      throw new InvalidHandshakeMessage();
    }
  }

  public void registerS2CGroupServer(S2CGroupServer s2cServer) {
    this.s2cGroupServers.put(s2cServer.groupId(), s2cServer);
  }

  private void propagate(FatalServerException e) {
    log.debug().setCause(e).log("S2CServer encountered a fatal error");
    s2cGroupServers.values().forEach(s -> s.fail(e));
  }

  @Override
  public void close() throws InterruptedException {
    if (state.get() == ServerState.CLOSED) {
      log.warn().log("Cannot stop S2CServer as it is already closed");
    }
    state.set(ServerState.CLOSED);
    log.info().log("Closing S2CServer");
    closeServerSocket();
    taskExecutor.close();
    s2cGroupServers.values().forEach(s -> {
      try {
        s.close();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e).log("Interrupted");
      }
    });
    s2cGroupServers.clear();
    log.info().log("S2CServer closed");
    joinLatch.countDown();

  }

  private void closeServerSocket() {
    if (serverSocket != null && !serverSocket.isClosed()) {
      try {
        serverSocket.close();
      }
      catch (IOException e) {
        ioErrorCounter.increment();
        log.debug().setCause(e).log("Error while closing socket");
      }
    }
  }

  public void join() throws InterruptedException {
    if (state.get() == ServerState.CLOSED) {
      return;
    }
    joinLatch.await();
  }

  private void initMetrics(MeterRegistry meterRegistry) {

    this.ioErrorCounter = Counter.builder("s2c.server.io.errors.count")
        .description("The count of IO errors encountered by the server")
        .register(meterRegistry);

    this.invalidHandshakesCounter = Counter.builder("s2c.server.invalid.handshakes.count")
        .description("The count of invalid handshakes made by clients")
        .register(meterRegistry);

    this.clientsWithUnknownGroupCounter = Counter.builder("s2c.clients.with.unknown.group.count")
        .description("The count of clients that are identified with unknown S2C groups")
        .register(meterRegistry);

    this.serverRestartsCounter = Counter.builder("s2c.server.restarts.count")
        .description("The number of times the server have restarted")
        .register(meterRegistry);

    this.handshakeTimer = Timer.builder("s2c.server.handshake.latency")
        .description(
            "The time between receving connection and successfully accepting the handshake")
        .register(meterRegistry);

    Gauge.builder("s2c.server.ready", () -> state.get() == ServerState.READY ? 1 : 0)
        .description("Whether the S2CServer is running.")
        .register(meterRegistry);

    Gauge.builder("s2c.server.restarting", () -> state.get() == ServerState.RESTARTING ? 1 : 0)
        .description("Whether the S2CServer is restarting.")
        .register(meterRegistry);
  }
}
