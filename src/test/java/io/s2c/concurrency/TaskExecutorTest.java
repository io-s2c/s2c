package io.s2c.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.TestUtil;

class TaskExecutorTest {

  private TaskExecutor taskExecutor;
  private MeterRegistry meterRegistry = new SimpleMeterRegistry();

  @BeforeEach
  void setUp() {
    taskExecutor = new TaskExecutor("test-owner", (t, e) -> {
    }, meterRegistry);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    taskExecutor.close();
  }
  


  @Test
  void testExecuteRunnable() throws InterruptedException {

    AtomicInteger counter = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);

    taskExecutor.execute(() -> {
      counter.incrementAndGet();
      latch.countDown();
    });

    assertTrue(latch.await(1, TimeUnit.SECONDS));
    assertEquals(1, counter.get());

  }

  @Test
  void testStartTask() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);
    taskExecutor.start("test-task", Task.of(() -> {
      counter.incrementAndGet();
      latch.countDown();
    }));
    assertTrue(latch.await(1, TimeUnit.SECONDS));
    assertEquals(1, counter.get());
  }

  @Test
  void testMaxConcurrency() throws InterruptedException {

    TaskExecutor limitedExecutor = new TaskExecutor("limited", (t, e) -> {
    }, 2, meterRegistry);

    AtomicInteger interruptions = new AtomicInteger();
    
    CountDownLatch l = new CountDownLatch(3);
    AtomicInteger accepted = new AtomicInteger(0);
    int rejectedCount = 0;

    // Start 3 tasks, but only 2 should run concurrently
    for (int i = 0; i < 3; i++) {
      CountDownLatch taskBlocker = new CountDownLatch(1);
      if (!limitedExecutor.tryStart("task-" + i, Task.of(() -> {
        accepted.incrementAndGet();
        l.countDown();
        try {
          taskBlocker.await();
        }
        catch (InterruptedException e1) {
          interruptions.incrementAndGet();
        }
      }, () -> taskBlocker.countDown()))) {
        rejectedCount++;
      }
    }

    boolean reachedThree = l.await(1, TimeUnit.SECONDS);

    assertFalse(reachedThree);
    assertEquals(1, rejectedCount);
    assertEquals(2, accepted.get());
    assertEquals(0, interruptions.get());
    limitedExecutor.close();
    limitedExecutor.join();
  }

  @Test
  void testTaskLifecycle() throws InterruptedException {

    AtomicInteger initiatedTasks = new AtomicInteger(0);

    AtomicInteger startedTasks = new AtomicInteger(0);

    AtomicInteger stoppedTasks = new AtomicInteger(0);
    
    TaskExecutor taskExecutor = new TaskExecutor("", (t, e) -> {}, meterRegistry);
    
    for (int i = 0; i < 3; i++) {
       taskExecutor.start("task-" + i, new Task() {

        CountDownLatch l;

        @Override
        public void close() throws InterruptedException {
          stoppedTasks.incrementAndGet();
          l.countDown();
        }

        @Override
        public void run() {
          try {
            startedTasks.incrementAndGet();
            l.await();
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }

        @Override
        public void init() {
          initiatedTasks.incrementAndGet();
          l = new CountDownLatch(1);
        }
      });
    }

    TestUtil.sleepUntil(1000, 50, () -> startedTasks.get() == 3);
    assertEquals(3, taskExecutor.currentTasksCount());
    assertEquals(3, initiatedTasks.get());
    taskExecutor.close();
    assertEquals(startedTasks.get(), stoppedTasks.get());
  }

  @Test
  void testMaxConcurrencyUnlimited() throws InterruptedException {
    TaskExecutor unlimited = new TaskExecutor("unlimited", (t, e) -> {
    }, meterRegistry);

    assertEquals(Integer.MAX_VALUE, unlimited.maxConcurrency());
    unlimited.close();
    unlimited.join();
  }

  @Test
  void testInvalidMaxConcurrency() {
    assertThrows(IllegalArgumentException.class, () -> {
      new TaskExecutor("invalid", (t, e) -> {
      }, 0, meterRegistry);
    });
  }

}
