package io.s2c.network;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.configs.S2CNetworkOptions;
import io.s2c.configs.S2COptions;
import io.s2c.configs.S2CRetryOptions;
import io.s2c.model.messages.Handshake;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.error.FatalServerException;
import io.s2c.network.message.reader.S2CMessageReader;

@ExtendWith(MockitoExtension.class)
class S2CServerTest {

  @Mock
  private S2CGroupServer groupServer;
  
  private S2CServer s2cServer;
  private NodeIdentity nodeIdentity;
  private MeterRegistry meterRegistry;
  private S2COptions s2cOptions;
  private S2CRetryOptions s2cRetryOptions;
  private Supplier<S2CMessageReader> messageReaderFactory;
  private ServerSocket testServerSocket;
  private int port;

  @BeforeEach
  void setUp() throws IOException {
    meterRegistry = new SimpleMeterRegistry();
    nodeIdentity = NodeIdentity.newBuilder()
        .setAddress("localhost")
        .setPort(0)
        .build();
    
    s2cOptions = new S2COptions();
    messageReaderFactory = () -> S2CMessageReader.create(1024 * 1024);
    
    testServerSocket = new ServerSocket(0);
    port = testServerSocket.getLocalPort();
    testServerSocket.close();
    
    nodeIdentity = NodeIdentity.newBuilder()
        .setAddress("localhost")
        .setPort(port)
        .build();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    if (s2cServer != null) {
      s2cServer.close();
    }
  }
  
  @Test
  void testCloseWithGroupServers() throws IOException, InterruptedException {
    
    s2cServer = new S2CServer(nodeIdentity, s2cOptions, meterRegistry);
    when(groupServer.groupId()).thenReturn("test-group");
    s2cServer.registerS2CGroupServer(groupServer);
    s2cServer.start();
    
    s2cServer.close();
    
    verify(groupServer).close();
  }


  @Test
  void testBindToPort() throws IOException {
    
    s2cServer = new S2CServer(nodeIdentity, s2cOptions, meterRegistry);
    s2cServer.start();
    
    assertThrows(IOException.class, () -> {
      try (ServerSocket testSocket = new ServerSocket(port)) {
        // Should throw
      }
    });
    
  }

  @Test
  void testHandleClientWithValidHandshake() throws IOException, InterruptedException {
    s2cServer = new S2CServer(nodeIdentity, s2cOptions, meterRegistry);
    when(groupServer.groupId()).thenReturn("test-group");
    s2cServer.registerS2CGroupServer(groupServer);
    s2cServer.start();
    
    // Create a client socket
    Socket clientSocket = new Socket("localhost", port);
    
    // Send handshake
    Handshake handshake = Handshake.newBuilder()
        .setGroupId("test-group")
        .setNodeIdentity(NodeIdentity.newBuilder()
            .setAddress("localhost")
            .setPort(8888)
            .build())
        .build();
    
    S2CMessage handshakeMsg = S2CMessage.newBuilder()
        .setCorrelationId("corr-1")
        .setHandshake(handshake)
        .build();
    
    DataOutputStream dout = new DataOutputStream(clientSocket.getOutputStream());
    dout.writeInt(handshakeMsg.getSerializedSize());
    handshakeMsg.writeTo(dout);
    dout.flush();
    
    // Give server time to process
    Thread.sleep(200);
    
    verify(groupServer).acceptClient(anyString(), any(Handshake.class), any(Socket.class));
    
    clientSocket.close();
  }

  @Test
  void testHandleClientWithUnknownGroup() throws IOException, InterruptedException {
    
    s2cServer = new S2CServer(nodeIdentity, s2cOptions, meterRegistry);
    when(groupServer.groupId()).thenReturn("test-group");
    s2cServer.registerS2CGroupServer(groupServer);
    s2cServer.start();
    
    Socket clientSocket = new Socket("localhost", port);
    
    Handshake handshake = Handshake.newBuilder()
        .setGroupId("unknown-group")
        .setNodeIdentity(NodeIdentity.newBuilder()
            .setAddress("localhost")
            .setPort(8888)
            .build())
        .build();
    
    S2CMessage handshakeMsg = S2CMessage.newBuilder()
        .setCorrelationId("corr-1")
        .setHandshake(handshake)
        .build();
    
    DataOutputStream dout = new DataOutputStream(clientSocket.getOutputStream());
    dout.writeInt(handshakeMsg.getSerializedSize());
    handshakeMsg.writeTo(dout);
    dout.flush();
    
    Thread.sleep(200);
    
    verify(groupServer, never()).acceptClient(anyString(), any(Handshake.class), any(Socket.class));
    
    clientSocket.close();
    
  }

}

