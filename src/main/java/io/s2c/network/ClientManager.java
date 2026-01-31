package io.s2c.network;

import static io.s2c.network.S2CGroupServer.POISON_PILL;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.s2c.ClientMessageAcceptor;
import io.s2c.ContextProvider;
import io.s2c.concurrency.Task;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.error.MessageTooLargeException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.message.reader.S2CMessageReader;

class ClientManager implements AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(ClientManager.class);
  private final StructuredLogger log;
  private final Client client;
  private final TaskExecutor taskExecutor;

  private final S2CMessageReader s2cMessageReader;
  private final Consumer<ClientManager> freeUp;
  private final ClientMessageAcceptor clientMessageAcceptor;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  public ClientManager(Client client,
      Consumer<ClientManager> freeUp,
      ContextProvider contextProvider,
      ClientMessageAcceptor clientMessageAcceptor,
      Supplier<S2CMessageReader> s2cMessageReaderFactory,
      MeterRegistry meterRegistry) {
    this.client = client;
    this.freeUp = freeUp;

    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.taskExecutor = new TaskExecutor(contextProvider.ownerName(ClientManager.class),
        log.uncaughtExceptionLogger(), meterRegistry);

    s2cMessageReader = s2cMessageReaderFactory.get();
    this.clientMessageAcceptor = clientMessageAcceptor;

    taskExecutor.start("message-reader", Task.of(() -> read(client)));

    taskExecutor.start("out-drainer", Task.of(() -> drainOut(client)));

  }

  private void read(Client client) {
    log.debug()
        .addKeyValue("nodeIdentity", () -> client.nodeIdentity().toString())
        .log("Listening for messages for client");
    DataInputStream din = new DataInputStream(client.in());
    while (!closed.get()) {
      S2CMessage message = null;
      try {
        message = s2cMessageReader.readNextMessage(din);
        clientMessageAcceptor.acceptIn(message);
      }
      catch (IOException e) {
        log.debug().setCause(e).log("Error while reading from socket");
        stopQuietly();
      }
      catch (MessageTooLargeException e) {
        throw new IllegalStateException(e);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e).log("Interrupted");

        stopQuietly();
      }
    }
  }

  private void drainOut(Client client) {
    DataOutputStream dout = new DataOutputStream(client.out());
    log.debug()
        .addKeyValue("nodeIdentity", () -> client.nodeIdentity().toString())
        .log("Started outbound drainer");
    while (!closed.get()) {
      try {
        S2CMessage message = clientMessageAcceptor.nextResponse();
        if (message == POISON_PILL) {
          break;
        }
        dout.writeInt(message.getSerializedSize());
        message.writeTo(dout);
      }
      catch (IOException e) {
        log.debug().setCause(e).log("Error while writing response. Stopping ClientManager");
        stopQuietly();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.debug().setCause(e).log("Interrupted");
        stopQuietly();
      }
    }
  }

  private void stopQuietly() {
    try {
      close();
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      log.debug().setCause(ex).log("Interrupted while stopping ClientManager");
    }
  }

  public NodeIdentity nodeIdentity() {
    return client.nodeIdentity();
  }

  @Override
  public void close() throws InterruptedException {
    if (closed.compareAndSet(false, true)) {
      log.trace()
          .addKeyValue("nodeIdentity", () -> client.nodeIdentity().toString())
          .log("Closing ClientManager");
      try {
        client.socket().close();
      }
      catch (IOException e) {
        log.debug().setCause(e).log("Error while closing socket");
      }
      freeUp.accept(this);
      taskExecutor.close();
      clientMessageAcceptor.close();
      log.debug()
          .addKeyValue("nodeIdentity", () -> client.nodeIdentity().toString())
          .log("ClientManager closed.");
    } else {
      log.debug().log("Cannot close ClientManager as it is already closed.");
    }
  }

}