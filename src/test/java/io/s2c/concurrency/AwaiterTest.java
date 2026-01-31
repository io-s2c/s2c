package io.s2c.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.s2c.error.S2CStoppedException;

class AwaiterTest {

  private Awaiter<String, S2CStoppedException> awaiter;

  @BeforeEach
  void setUp() {
    awaiter = new Awaiter<>(S2CStoppedException::new);
  }

  @Test
  void testAcceptAndAwait() throws InterruptedException, S2CStoppedException {

    CompletableFuture.runAsync(() -> {
      try {
        awaiter.accept("test-value");
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    String result = awaiter.await(value -> true);
    assertEquals("test-value", result);

  }

  @Test
  void testAwaitWithPredicate() throws InterruptedException, S2CStoppedException {

    CompletableFuture.runAsync(() -> {
      try {
        awaiter.accept("skip1");
        awaiter.accept("skip2");
        awaiter.accept("match");
        awaiter.accept("skip3");
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    String result = awaiter.await(value -> value.equals("match"));
    assertEquals("match", result);

  }

  @Test
  void testStop() throws InterruptedException {

    AtomicBoolean stopped = new AtomicBoolean();
    CountDownLatch latch = new CountDownLatch(1);

    CompletableFuture.runAsync(() -> {
      try {
        awaiter.await(value -> true);
      }
      catch (S2CStoppedException e) {
        stopped.set(true);
        latch.countDown();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    awaiter.stop();
    latch.await();
    assertTrue(stopped.get());

  }

  @Test
  void testStopPreventsAccept() throws InterruptedException {

    awaiter.stop();

    awaiter.accept("test");

    // Await should throw immediately
    assertThrows(S2CStoppedException.class, () -> {
      awaiter.await(value -> true);
    });
  }

  @Test
  void testConcurrentAccept() throws InterruptedException, S2CStoppedException {

    int numThreads = 10;
    int i = 0;
    while (i < 10) {
      CompletableFuture.runAsync(() -> {
        try {
          awaiter.accept("value");
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      i++;
    }
    while (numThreads > 0) {
      String result = awaiter.await(value -> true);
      assertTrue(result.startsWith("value"));
      numThreads--;
    }
  }

  @Test
  void testCustomExceptionSupplier() throws InterruptedException {
    Awaiter<String, RuntimeException> customAwaiter = new Awaiter<>(
        () -> new RuntimeException("Custom"));

    customAwaiter.stop();

    assertThrows(RuntimeException.class, () -> {
      customAwaiter.await(value -> true);
    });
  }

  @Test
  void testMultipleAwaiters() throws InterruptedException, S2CStoppedException {
    
    Awaiter<String, S2CStoppedException> awaiter1 = new Awaiter<>(S2CStoppedException::new);
    Awaiter<String, S2CStoppedException> awaiter2 = new Awaiter<>(S2CStoppedException::new);

    

    CompletableFuture.runAsync(() -> {
      try {
        awaiter1.accept("value1");
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    CompletableFuture.runAsync(() -> {
      try {
        awaiter2.accept("value2");
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    String result1 = awaiter1.await(value -> true);
    String result2 = awaiter2.await(value -> true);

    assertEquals("value1", result1);
    assertEquals("value2", result2);

  }

  @Test
  void testAwaitBlocksUntilMatching() throws InterruptedException, S2CStoppedException {

    long startTime = System.currentTimeMillis();

    CompletableFuture.runAsync(() -> {
      try {
        awaiter.accept("non-matching-value");
        Thread.sleep(200);
        awaiter.accept("delayed-value");
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    String result = awaiter.await(value -> value.equals("delayed-value"));
    long elapsed = System.currentTimeMillis() - startTime;

    assertEquals("delayed-value", result);
    assertTrue(elapsed >= 150); // Should have waited

  }
}
