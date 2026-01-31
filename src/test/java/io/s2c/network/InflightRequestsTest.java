package io.s2c.network;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.concurrency.RequestResponseTask;
import io.s2c.model.messages.S2CMessage;
import io.s2c.network.error.ClientException;

class InflightRequestsTest {

  private InflightRequests inflightRequests;
  private MeterRegistry meterRegistry;

  @BeforeEach
  void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    inflightRequests = new InflightRequests(meterRegistry);
  }

  @Test
  void testAddAndGet() {
    S2CMessage request = S2CMessage.newBuilder()
        .setCorrelationId("corr-1")
        .build();
    
    RequestResponseTask<S2CMessage, S2CMessage, ClientException> task = 
        new RequestResponseTask<>(request);
    
    inflightRequests.add(task);
    
    RequestResponseTask<S2CMessage, S2CMessage, ClientException> retrieved = 
        inflightRequests.get("corr-1");
    
    assertEquals(task, retrieved);
  }

  @Test
  void testGetNonExistent() {
    assertNull(inflightRequests.get("nonexistent"));
  }


  @Test
  void testDiscardAndRemove() {
    S2CMessage request = S2CMessage.newBuilder()
        .setCorrelationId("corr-1")
        .build();
    
    RequestResponseTask<S2CMessage, S2CMessage, ClientException> task = 
        new RequestResponseTask<>(request);
    
    inflightRequests.add(task);
    assertEquals(1, inflightRequests.size());
    
    inflightRequests.discardAndRemove("corr-1");
    assertEquals(0, inflightRequests.size());
  }

  @Test
  void testRespondToRequest() {
    S2CMessage request = S2CMessage.newBuilder()
        .setCorrelationId("corr-1")
        .build();
    
    RequestResponseTask<S2CMessage, S2CMessage, ClientException> task = 
        new RequestResponseTask<>(request);
    
    inflightRequests.add(task);
    
    S2CMessage response = S2CMessage.newBuilder()
        .setCorrelationId("corr-1")
        .build();
    
    inflightRequests.respondToRequest(response);
    
    assertEquals(response, task.response());
  }

  @Test
  void testFail() {
    S2CMessage request1 = S2CMessage.newBuilder()
        .setCorrelationId("corr-1")
        .build();
    S2CMessage request2 = S2CMessage.newBuilder()
        .setCorrelationId("corr-2")
        .build();
    
    RequestResponseTask<S2CMessage, S2CMessage, ClientException> task1 = 
        new RequestResponseTask<>(request1);
    RequestResponseTask<S2CMessage, S2CMessage, ClientException> task2 = 
        new RequestResponseTask<>(request2);
    
    inflightRequests.add(task1);
    inflightRequests.add(task2);
    
    ClientException exception = new ClientException(new IOException());
    inflightRequests.fail(exception);
    
    assertEquals(exception, task1.excption());
    assertEquals(exception, task2.excption());
  }

  @Test
  void testClear() {
    S2CMessage request1 = S2CMessage.newBuilder()
        .setCorrelationId("corr-1")
        .build();
    S2CMessage request2 = S2CMessage.newBuilder()
        .setCorrelationId("corr-2")
        .build();
    
    RequestResponseTask<S2CMessage, S2CMessage, ClientException> task1 = 
        new RequestResponseTask<>(request1);
    RequestResponseTask<S2CMessage, S2CMessage, ClientException> task2 = 
        new RequestResponseTask<>(request2);
    
    inflightRequests.add(task1);
    inflightRequests.add(task2);
    assertEquals(2, inflightRequests.size());
    
    inflightRequests.clear();
    assertEquals(0, inflightRequests.size());
  }

}

