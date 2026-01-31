package io.s2c.network;

import java.io.IOException;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.s2c.ClientMessageAcceptor;
import io.s2c.ContextProvider;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.Handshake;
import io.s2c.model.messages.S2CMessage;
import io.s2c.network.error.FatalServerException;
import io.s2c.network.message.reader.S2CMessageReader;

public class S2CGroupServer implements AutoCloseable {

  public static final S2CMessage POISON_PILL = S2CMessage.getDefaultInstance();

  private final Logger logger = LoggerFactory.getLogger(S2CGroupServer.class);
  private final StructuredLogger log;

  private final Set<ClientManager> clientManagers = ConcurrentHashMap.newKeySet();

  private volatile boolean closed = false;

  private final ContextProvider contextProvider;
  private final Supplier<ClientMessageAcceptor> clientMessageAcceptorFactory;
  private final Supplier<S2CMessageReader> s2cMessageReaderFactory;

  private final String groupId;
  private final BiConsumer<S2CGroupServer, FatalServerException> handleFatalServerException;

  private final MeterRegistry meterRegistry;

  private final ReentrantLock acceptLock = new ReentrantLock();

  public S2CGroupServer(Supplier<ClientMessageAcceptor> clientMessageAcceptorFactory,
      String groupId,
      BiConsumer<S2CGroupServer, FatalServerException> handleFatalServerException,
      Supplier<S2CMessageReader> s2cMessageReaderFactory,
      ContextProvider contextProvider,
      MeterRegistry meterRegistry) {

    this.clientMessageAcceptorFactory = clientMessageAcceptorFactory;
    this.s2cMessageReaderFactory = s2cMessageReaderFactory;
    this.groupId = groupId;
    this.handleFatalServerException = handleFatalServerException;
    this.contextProvider = contextProvider;
    this.meterRegistry = meterRegistry;
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());

    Gauge.builder("s2c.group.server.running", () -> isRunning() ? 1 : 0)
        .tag("s2cGroupId", groupId)
        .register(meterRegistry);

    Gauge.builder("s2c.group.server.active.clients", clientManagers::size)
        .tag("s2cGroupId", groupId)
        .register(meterRegistry);

  }

  private boolean isRunning() {
    return !closed;
  }

  private void handleClient(String correlationId, Handshake handhshake, Socket socket)
      throws IOException, InterruptedException {

    Client client = newClient(correlationId, handhshake, socket);
    var clientMessageAcceptor = clientMessageAcceptorFactory.get();

    var handshakeMsg = S2CMessage.newBuilder()
        .setCorrelationId(correlationId)
        .setHandshake(handhshake)
        .build();

    clientMessageAcceptor.acceptIn(handshakeMsg);

    ClientManager clientManager = new ClientManager(client, clientManagers::remove, contextProvider,
        clientMessageAcceptor, s2cMessageReaderFactory, meterRegistry);

    clientManagers.add(clientManager);

    log.debug().addKeyValue("correlationId", correlationId).log("Serving client.");
  }

  public void acceptClient(String correlationId, Handshake handhshake, Socket socket)
      throws IOException, InterruptedException {
    try {
      acceptLock.lock();
      if (!closed) {
        handleClient(correlationId, handhshake, socket);
      } else {
        log.warn().log("Cannot accept client as S2CGroupServer is closed.");
      }
    }
    finally {
      acceptLock.unlock();
    }
  }

  private Client newClient(String correlationId, Handshake handshake, Socket socket)
      throws IOException {
    socket.setSoTimeout(0);
    return new Client(socket, socket.getInputStream(), socket.getOutputStream(),
        handshake.getNodeIdentity());
  }

  public String groupId() {
    return groupId;
  }

  public void fail(FatalServerException e) {
    try {
      close();
      handleFatalServerException.accept(this, e);
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      log.debug().setCause(ex).log("Interrupted");

    }
  }

  @Override
  public void close() throws InterruptedException {
    try {
      acceptLock.lock();
      if (!closed) {
        closed = true;
        for (ClientManager cm : clientManagers) {
          cm.close();
        }
        clientManagers.clear();
      } else {
        log.warn().log("Cannot close S2CGroupServer because it is already closed.");
      }
    }
    finally {
      acceptLock.unlock();
    }
  }
}