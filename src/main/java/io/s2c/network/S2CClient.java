package io.s2c.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.s2c.ContextProvider;
import io.s2c.concurrency.RequestResponseTask;
import io.s2c.concurrency.Task;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.configs.S2COptions;
import io.s2c.error.ConnectTimeoutException;
import io.s2c.error.HandshakeTimeoutException;
import io.s2c.error.MessageTooLargeException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.Handshake;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.error.ClientException;
import io.s2c.network.error.ClientNotConnectedException;
import io.s2c.network.error.ClientStoppedException;
import io.s2c.network.error.UnknownHostException;
import io.s2c.network.message.reader.S2CMessageReader;

public class S2CClient implements AutoCloseable {

  // NOT_CONNECTED -> CONNECTED -> READY -> CLOSED
  public enum ClientState {
    NOT_CONNECTED, CONNECTED, READY, CLOSED
  }

  private final int connectTimeoutMs;
  private final ContextProvider contextProvider;
  private DataInputStream din;

  private DataOutputStream dout;

  private final StructuredLogger log;
  private final Logger logger = LoggerFactory.getLogger(S2CClient.class);
  private final BlockingQueue<RequestResponseTask<S2CMessage, S2CMessage, ClientException>> out;
  private final RequestResponseTask<S2CMessage, S2CMessage, ClientException> POISON_PILL = new RequestResponseTask<>(
      null);
  private final S2CMessageReader s2cMessageReader;

  private final int throttleDelayMs;
  private final InflightRequests inflightRequests;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final MeterRegistry meterRegistry;
  private final Consumer<ClientState> stateChangeConsumer;
  private final ClientRole clientRole;
  private S2CConnection s2cConnection;
  private volatile boolean throttle = false;

  private Counter ioErrorCounter;
  private TaskExecutor taskExecutor;

  private volatile ClientState clientState = ClientState.NOT_CONNECTED;

  public S2CClient(S2COptions s2cOptions,
      ContextProvider contextProvider,
      MeterRegistry meterRegistry,
      Consumer<ClientState> stateChangeConsumer,
      Supplier<S2CMessageReader> s2cMessageReaderFactory,
      ClientRole clientRole) {
    this.connectTimeoutMs = s2cOptions.s2cNetworkOptions().connectTimeoutMs();
    this.throttleDelayMs = s2cOptions.s2cNetworkOptions().throttleDelayMs();
    this.s2cMessageReader = s2cMessageReaderFactory.get();
    this.contextProvider = contextProvider;
    this.meterRegistry = meterRegistry;
    this.inflightRequests = new InflightRequests(meterRegistry);
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.out = new LinkedBlockingQueue<>(s2cOptions.s2cNetworkOptions().maxPendingClientReqs());
    this.stateChangeConsumer = stateChangeConsumer;
    this.clientRole = clientRole;
    initMetrics(clientRole);

  }

  public void connect(NodeIdentity serverNodeIdentity) throws IOException, UnknownHostException,
      ConnectTimeoutException, InterruptedException, ClientNotConnectedException {
    lock.writeLock().lockInterruptibly();
    try {
      if (clientState() == ClientState.NOT_CONNECTED) {
        log.trace().log("Client connecting");
        this.s2cConnection = S2CConnection.of(serverNodeIdentity, connectTimeoutMs);
        din = new DataInputStream(s2cConnection.in());
        dout = new DataOutputStream(s2cConnection.out());
        clientState(ClientState.CONNECTED);
        this.taskExecutor = new TaskExecutor(roleAwareName(clientRole),
            log.uncaughtExceptionLogger(), meterRegistry);
        taskExecutor.start("drain-in", Task.of(this::drainIn));
        taskExecutor.start("drain-out", Task.of(this::drainOut, () -> out.put(POISON_PILL)));
        handshake();
        log.debug().log("Handshake succesful.");
        clientState(ClientState.READY);
        log.debug().log("S2Client ready");
      } else {
        log.warn().addKeyValue("clientState", clientState()).log("Wrong state for connect.");
      }
    }
    catch (HandshakeTimeoutException e) {
      reset();
      throw new ConnectTimeoutException(e);
    }
    catch (IOException e) {
      reset();
      throw e;
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public S2CMessage send(S2CMessage s2cMessage, long timeoutMS) throws InterruptedException,
      IOException, ClientStoppedException, TimeoutException, ClientNotConnectedException {

    // Force await if connecting
    lock.readLock().lockInterruptibly();
    try {

      // To force re-connect
      if (clientState() == ClientState.NOT_CONNECTED) {
        throw new ClientNotConnectedException();
      }

      // Node is shutting down
      if (clientState() == ClientState.CLOSED) {
        throw new ClientStoppedException();
      }

    }
    finally {
      lock.readLock().unlock();
    }

    return sendAndAwaitResponse(s2cMessage, timeoutMS);

  }

  private S2CMessage handshakeMessage() {
    return S2CMessage.newBuilder()
        .setCorrelationId(UUID.randomUUID().toString())
        .setHandshake(Handshake.newBuilder()
            .setGroupId(contextProvider.s2cGroupId())
            .setNodeIdentity(contextProvider.nodeIdentity()))
        .build();
  }

  private void closeConnection() {
    try {
      if (s2cConnection != null) {
        s2cConnection.close();
      }
    }
    catch (IOException e) {
      log.debug().setCause(e).log("Error while closing connection");
    }

  }

  private void failAndClearSendRequests(ClientException cause) {
    inflightRequests.fail(cause);
  }

  private void handshake() throws IOException, InterruptedException, HandshakeTimeoutException,
      ClientNotConnectedException {
    try {
      S2CMessage s2cMessage = handshakeMessage();
      S2CMessage res = sendAndAwaitResponse(s2cMessage, connectTimeoutMs);

      if (!res.hasHandshake()) {
        throw new IllegalStateException("Invalid response for handshake");
      }
    }
    catch (TimeoutException e) {
      throw new HandshakeTimeoutException(e);
    }
    catch (ClientStoppedException ignore) {
      // Non-reachable
    }
  }

  private void drainOut() {
    while (running()) {
      try {
        sendPendingRequest();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e).log("Interrupted");
        break;
      }

    }
  }

  private boolean running() {
    return EnumSet.of(ClientState.CONNECTED, ClientState.READY).contains(clientState());
  }

  private void drainIn() {
    while (running()) {
      try {
        readNextMessage();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e).log("Interrupted");

        break;
      }
      catch (MessageTooLargeException e) {
        throw new IllegalStateException(e);
      }
      catch (IOException e) {
        log.debug().setCause(e).log("Error in drainIn loop");
        handleIOException(e);
        break;
      }
    }
  }

  private void readNextMessage() throws InterruptedException, EOFException,
      InvalidProtocolBufferException, IOException, MessageTooLargeException {

    S2CMessage res = s2cMessageReader.readNextMessage(din);

    if (running()) {
      throttle = res.hasSlowDownError();
      respondInflight(res);
    }

  }

  private void respondInflight(S2CMessage res) {
    inflightRequests.respondToRequest(res);
  }

  private S2CMessage sendAndAwaitResponse(S2CMessage s2cMessage, long timeoutMS)
      throws InterruptedException, TimeoutException, IOException, ClientStoppedException,
      ClientNotConnectedException {
    RequestResponseTask<S2CMessage, S2CMessage, ClientException> reqRes = new RequestResponseTask<>(
        s2cMessage);
    try {

      out.put(reqRes);

      S2CMessage response = reqRes.await(timeoutMS, TimeUnit.MILLISECONDS);
      if (response == null) {
        inflightRequests.discardAndRemove(reqRes.request().getCorrelationId());
        throw new TimeoutException();
      }
      return response;
    }
    catch (ClientException e) {
      inflightRequests.discardAndRemove(reqRes.request().getCorrelationId());
      // To enforce compile time handling of the real cause
      e.throwCause();
      // Unreachable
      return null;
    }
  }

  private void sendOut(DataOutputStream dout,
      final RequestResponseTask<S2CMessage, S2CMessage, ClientException> reqRes)
      throws InterruptedException {

    try {
      dout.writeInt(reqRes.request().getSerializedSize());
      reqRes.request().writeTo(dout);
      inflightRequests.add(reqRes);
    }
    catch (IOException e) {
      reqRes.exception(new ClientException(e));
      log.debug().setCause(e).log("Error while sending message");
      handleIOException(e);
    }

  }

  private void handleIOException(IOException e) {
    ioErrorCounter.increment();
    if (Thread.currentThread().isInterrupted()) {
      log.debug().setCause(e).log("Socket closed by interrupt");

    }
    try {
      fail(new ClientException(e));
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      log.debug().setCause(ex).log("Interrupted");
    }
  }

  private void sendPendingRequest() throws InterruptedException {
    final var reqRes = out.take();
    // While the poison pill doens't directly break
    // the drain-out loop, it is still used to wake up the thread.
    if (reqRes != POISON_PILL) {
      if (throttle) {
        TimeUnit.MILLISECONDS.sleep(throttleDelayMs);
      }
      if (throttle) {
        throttle = false;
      }
      if (running()) {
        sendOut(dout, reqRes);
      }
    }

  }

  public void disconnect() throws InterruptedException {
    fail(new ClientException(new ClientNotConnectedException()));
  }

  private void reset() throws InterruptedException {
    // Lock acquired in connect()
    inflightRequests.clear();
    clientState(ClientState.NOT_CONNECTED);
    freeResources();
  }

  private void fail(ClientException e) throws InterruptedException {
    lock.writeLock().lockInterruptibly();
    try {
      if (clientState() == ClientState.READY) {
        failAndClearSendRequests(e);
        freeResources();
        clientState(ClientState.NOT_CONNECTED);
      }
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws InterruptedException {
    lock.writeLock().lockInterruptibly();
    try {
      if (clientState() != ClientState.CLOSED) {
        log.trace().log("Closing S2CClient");
        clientState(ClientState.CLOSED);
        failAndClearSendRequests(new ClientException(new ClientStoppedException()));
        inflightRequests.clear();
        freeResources();
        log.debug().log("S2CClient closed");
      } else {
        log.warn().log("S2CClient canot be closed as it is already closed");
      }
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  private void freeResources() throws InterruptedException {
    closeConnection(); // Unblock drain-in first
    if (taskExecutor != null) {
      taskExecutor.close();
    }
  }

  public boolean isReady() throws InterruptedException {
    lock.readLock().lockInterruptibly();
    try {
      return clientState() == ClientState.READY;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  private void initMetrics(ClientRole clientRole) {

    Supplier<Integer> isReadyQuietly = () -> {
      try {
        return isReady() ? 1 : 0;
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e).log("Interrupted");
        return 0;
      }
    };

    Gauge.builder("s2c.client.ready", isReadyQuietly)
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .tag("clientRole", clientRole.name())
        .description("Whether the client to the server is ready")
        .register(meterRegistry);

    Gauge.builder("s2c.client.inflight", inflightRequests::size)
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .tag("clientRole", clientRole.name())
        .description("Count of inflight requests")
        .register(meterRegistry);

    Gauge.builder("s2c.client.queue", out::size)
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .tag("clientRole", clientRole.name())
        .description("Count of queued outgoing requests")
        .register(meterRegistry);

    ioErrorCounter = Counter.builder("s2c.client.io.errors")
        .description("Count of encountered IO errors")
        .register(meterRegistry);

  }

  public ClientState clientState() {
    return clientState;
  }

  private void clientState(ClientState clientState) {
    this.clientState = clientState;
    this.stateChangeConsumer.accept(clientState);
  }

  private String roleAwareName(ClientRole clientRole) {
    return "%s-%s".formatted(contextProvider.ownerName(S2CClient.class), clientRole.name());
  }
}