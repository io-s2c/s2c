package io.s2c.network;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.Socket;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.ClientMessageAcceptor;
import io.s2c.ContextProvider;
import io.s2c.model.messages.Handshake;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.error.FatalServerException;
import io.s2c.network.message.reader.S2CMessageReader;

@ExtendWith(MockitoExtension.class)
class S2CGroupServerTest {

  @Mock
  private ClientMessageAcceptor clientMessageAcceptor;

  @Mock
  private BiConsumer<S2CGroupServer, FatalServerException> fatalErrorHandler;

  private S2CGroupServer groupServer;
  private ContextProvider contextProvider;
  private MeterRegistry meterRegistry;
  private String groupId;
  private Supplier<ClientMessageAcceptor> acceptorFactory;
  private Supplier<S2CMessageReader> messageReaderFactory;
  private NodeIdentity nodeIdentity;

  @BeforeEach
  void setUp() {

    meterRegistry = new SimpleMeterRegistry();

    groupId = "test-group";

    nodeIdentity = NodeIdentity.newBuilder()
        .setAddress("localhost")
        .setPort(7777)
        .build();

    contextProvider = new ContextProvider(groupId, nodeIdentity, false);

    acceptorFactory = () -> clientMessageAcceptor;
    messageReaderFactory = () -> S2CMessageReader.create(1024 * 1024);

    groupServer = new S2CGroupServer(
        acceptorFactory,
        groupId,
        fatalErrorHandler,
        messageReaderFactory,
        contextProvider,
        meterRegistry);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    if (groupServer != null) {
      groupServer.close();
    }
  }

  @Test
  void testGroupId() {
    assertEquals(groupId, groupServer.groupId());
  }

  @Test
  void testAcceptClient() throws IOException, InterruptedException {
    String correlationId = "corr-1";
    Handshake handshake = Handshake.newBuilder()
        .setGroupId(groupId)
        .setNodeIdentity(nodeIdentity)
        .build();

    Socket socket = mock(Socket.class);
    when(socket.getInputStream()).thenReturn(mock(java.io.InputStream.class));
    when(socket.getOutputStream()).thenReturn(mock(java.io.OutputStream.class));

    groupServer.acceptClient(correlationId, handshake, socket);

    verify(clientMessageAcceptor).acceptIn(any());
  }

  @Test
  void testAcceptClientWhenClosed() throws IOException, InterruptedException {

    groupServer.close();

    String correlationId = "corr-1";
    Handshake handshake = Handshake.newBuilder()
        .setGroupId(groupId)
        .setNodeIdentity(nodeIdentity)
        .build();

    Socket socket = mock(Socket.class);

    groupServer.acceptClient(correlationId, handshake, socket);

    verify(clientMessageAcceptor, never()).acceptIn(any());
  }

  @Test
  void testFail() {
    FatalServerException fatalError = new FatalServerException(new IOException("test error"));

    groupServer.fail(fatalError);

    verify(fatalErrorHandler).accept(any(S2CGroupServer.class), any(FatalServerException.class));
  }

  @Test
  void testFailClosesServer() throws InterruptedException, IOException {
    FatalServerException fatalError = new FatalServerException(new IOException("test error"));

    groupServer.fail(fatalError);

    String correlationId = "corr-1";
    Handshake handshake = Handshake.newBuilder()
        .setGroupId(groupId)
        .setNodeIdentity(nodeIdentity)
        .build();

    Socket socket = mock(Socket.class);

    groupServer.acceptClient(correlationId, handshake, socket);

    verify(clientMessageAcceptor, never()).acceptIn(any());
  }

}
