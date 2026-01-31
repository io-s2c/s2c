package io.s2c.network;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.ContextProvider;
import io.s2c.configs.S2COptions;
import io.s2c.error.ConnectTimeoutException;
import io.s2c.model.messages.Handshake;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.messages.StateRequest;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.error.ClientNotConnectedException;
import io.s2c.network.error.ClientStoppedException;
import io.s2c.network.error.UnknownHostException;
import io.s2c.network.message.reader.S2CMessageReader;

class S2CClientTest {

  private S2CClient s2cClient;
  private ContextProvider contextProvider;
  private MeterRegistry meterRegistry;
  private S2COptions s2cOptions;
  private Supplier<S2CMessageReader> messageReaderFactory;
  private NodeIdentity serverIdentity;
  private ServerSocket serverSocket;
  private int port;
  private AtomicReference<S2CClient.ClientState> stateRef;

  @BeforeEach
  void setUp() throws IOException {
    meterRegistry = new SimpleMeterRegistry();
    s2cOptions = new S2COptions();
    messageReaderFactory = () -> S2CMessageReader.create(1024 * 1024);
    
    NodeIdentity clientIdentity = NodeIdentity.newBuilder()
        .setAddress("localhost")
        .setPort(7777)
        .build();
    contextProvider = new ContextProvider("test-group", clientIdentity, false);
    
    serverSocket = new ServerSocket(0);
    port = serverSocket.getLocalPort();
    serverIdentity = NodeIdentity.newBuilder()
        .setAddress("localhost")
        .setPort(port)
        .build();
    
    stateRef = new AtomicReference<>(S2CClient.ClientState.NOT_CONNECTED);
    Consumer<S2CClient.ClientState> stateChangeConsumer = stateRef::set;
    
    s2cClient = new S2CClient(
        s2cOptions,
        contextProvider,
        meterRegistry,
        stateChangeConsumer,
        messageReaderFactory,
        ClientRole.FOLLOWER
    );
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    if (s2cClient != null) {
      s2cClient.close();
    }
    if (serverSocket != null && !serverSocket.isClosed()) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        // Ignore
      }
    }
  }

  @Test
  void testInitialState() {
    assertEquals(S2CClient.ClientState.NOT_CONNECTED, s2cClient.clientState());
    try {
      assertFalse(s2cClient.isReady());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  void testConnect() throws Exception {
    
    CountDownLatch l = new CountDownLatch(1);
    
    CompletableFuture.runAsync(() -> {
      try {
        l.countDown();
        Socket clientSocket = serverSocket.accept();
        DataInputStream din = new DataInputStream(clientSocket.getInputStream());
        DataOutputStream dout = new DataOutputStream(clientSocket.getOutputStream());
        
        // Read handshake
        int size = din.readInt();
        byte[] data = new byte[size];
        din.readFully(data);
        S2CMessage handshake = S2CMessage.parseFrom(data);
        
        // Echo handshake back
        S2CMessage response = S2CMessage.newBuilder()
            .setCorrelationId(handshake.getCorrelationId())
            .setHandshake(handshake.getHandshake())
            .build();
        
        dout.writeInt(response.getSerializedSize());
        response.writeTo(dout);
        dout.flush();
        
      } catch (Exception e) {
        // Ignore
      }
    });
    
    l.await();
    
    s2cClient.connect(serverIdentity);
    
    assertEquals(S2CClient.ClientState.READY, s2cClient.clientState());
    assertTrue(s2cClient.isReady());
    
  }

  @Test
  void testConnectTimeout() {
    NodeIdentity unreachableIdentity = NodeIdentity.newBuilder()
        .setAddress("192.0.2.1") // Non-routable IP
        .setPort(12345)
        .build();
    
    assertThrows(ConnectTimeoutException.class, () -> {
      s2cClient.connect(unreachableIdentity);
    });
  }

  @Test
  void testConnectUnknownHost() {
    NodeIdentity unknownHostIdentity = NodeIdentity.newBuilder()
        .setAddress("nonexistent-host-12345.invalid")
        .setPort(12345)
        .build();
    
    assertThrows(UnknownHostException.class, () -> {
      s2cClient.connect(unknownHostIdentity);
    });
  }

  @Test
  void testSendWhenNotConnected() {
    S2CMessage message = S2CMessage.newBuilder()
        .setCorrelationId("corr-1")
        .build();
    
    assertThrows(ClientNotConnectedException.class, () -> {
      s2cClient.send(message, 1000);
    });
  }

  @Test
  void testSendWhenClosed() throws InterruptedException {
    
    s2cClient.close();
    S2CMessage message = S2CMessage.newBuilder()
        .setCorrelationId("corr-1")
        .build();
    
    assertThrows(ClientStoppedException.class, () -> {
      s2cClient.send(message, 1000);
    });
  }


  @Test
  void testClose() throws InterruptedException {
    
    s2cClient.close();
    assertEquals(S2CClient.ClientState.CLOSED, s2cClient.clientState());
    assertFalse(s2cClient.isReady());
    
  }
}

