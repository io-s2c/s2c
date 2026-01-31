package io.s2c.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.s2c.network.error.ClientException;

class RequestResponseTaskTest {

  private RequestResponseTask<String, Integer, ClientException> task;

  @BeforeEach
  void setUp() {
    task = new RequestResponseTask<>("test-request");
  }

  @Test
  void testRequest() {
    assertEquals("test-request", task.request());
  }

  @Test
  void testResponseInitiallyNull() {
    assertNull(task.response());
  }

  @Test
  void testResponse() throws InterruptedException, ClientException {
    task.response(42);
    assertEquals(42, task.response());
    assertEquals(42, task.await());
  }

  @Test
  void testException() {

    ClientException ex = new ClientException(new IOException());
    task.exception(ex);

    assertEquals(ex, task.excption());
    assertThrows(ClientException.class, () -> {
      try {
        task.await();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
  }

  @Test
  void testAwait() throws InterruptedException, ClientException {

    CompletableFuture.runAsync(() -> {
      task.response(100);
    });

    Integer result = task.await();
    assertEquals(100, result);
    assertTrue(task.finished());
  }

  @Test
  void testAwaitWithTimeout() throws InterruptedException, ClientException {
    ExecutorService executor = Executors.newSingleThreadExecutor();

    executor.submit(() -> {
      task.response(200);

    });
    Integer result = task.await(1, TimeUnit.SECONDS);
    assertEquals(200, result);
    assertTrue(task.finished());

    executor.shutdownNow();
  }

  @Test
  void testAwaitWithException() {
    
    ClientException ex = new ClientException(new IOException());
    task.exception(ex);

    assertThrows(ClientException.class, () -> {
      try {
        task.await();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    assertTrue(task.finished());
  }


  @Test
  void testResponseAfterException() {
    ClientException ex = new ClientException(new IOException());
    task.exception(ex);
    task.response(999); // Should not override exception

    assertEquals(ex, task.excption());
    assertThrows(ClientException.class, () -> {
      try {
        task.await();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
  }
}
