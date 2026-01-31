package io.s2c.network;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.s2c.error.ConnectTimeoutException;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.error.UnknownHostException;

class S2CConnectionTest {

  private ServerSocket serverSocket;
  private int port;

  @BeforeEach
  void setUp() throws IOException {
    serverSocket = new ServerSocket(0);
    port = serverSocket.getLocalPort();
  }

  @AfterEach
  void tearDown() throws IOException {
    if (serverSocket != null && !serverSocket.isClosed()) {
      serverSocket.close();
    }
  }

  @Test
  void testConnect() throws IOException, UnknownHostException, ConnectTimeoutException, InterruptedException {
    NodeIdentity nodeIdentity = NodeIdentity.newBuilder()
        .setAddress("localhost")
        .setPort(port)
        .build();
    
    CountDownLatch l = new CountDownLatch(1);
    CompletableFuture.runAsync(() -> {
      try {
        l.countDown();
        serverSocket.accept();
      } catch (IOException e) {
        // Ignore
      }
    });
    
    l.await();
    S2CConnection connection = S2CConnection.of(nodeIdentity, 1000);
    
    assertTrue(connection.isConnected());
    assertFalse(connection.isClosed());
    
    connection.close();
    assertTrue(connection.isClosed());
  }

  @Test
  void testInOutStreams() throws IOException, UnknownHostException, ConnectTimeoutException {
    
    NodeIdentity nodeIdentity = NodeIdentity.newBuilder()
        .setAddress("localhost")
        .setPort(port)
        .build();
    
    CompletableFuture.runAsync(() -> {
      try {
        serverSocket.accept();
      } catch (IOException e) {
        // Ignore
      }
    });
    
    S2CConnection connection = S2CConnection.of(nodeIdentity, 1000);
    assertNotNull(connection.in());
    assertNotNull(connection.out());
    
    connection.close();
    assertTrue(connection.isClosed());

  }
}

