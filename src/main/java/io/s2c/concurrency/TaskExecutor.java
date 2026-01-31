package io.s2c.concurrency;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class TaskExecutor implements Executor, AutoCloseable {

  private final ExecutorService executorService;
  private final String owner;

  private final AtomicInteger unnamedTasksCounter = new AtomicInteger();
  private final AtomicInteger waitingTasks = new AtomicInteger();

  private final Semaphore semaphore;

  private final Set<Task> tasks = ConcurrentHashMap.newKeySet();
  private final int maxConcurrency;

  private final CountDownLatch joinLatch = new CountDownLatch(1);

  private final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

  public TaskExecutor(String owner,
      UncaughtExceptionHandler uncaughtExceptionHandler,
      MeterRegistry meterRegistry) {
    // No limit if not specified
    this(owner, uncaughtExceptionHandler, Integer.MAX_VALUE, meterRegistry);
  }

  public TaskExecutor(String owner,
      UncaughtExceptionHandler uncaughtExceptionHandler,
      int maxConcurrency,
      MeterRegistry meterRegistry) {
    Objects.requireNonNull(owner);
    Objects.requireNonNull(uncaughtExceptionHandler);

    if (maxConcurrency <= 0) {
      throw new IllegalArgumentException("maxConcurrency must be greater than zero");
    }
    executorService = Executors.newThreadPerTaskExecutor(
        Thread.ofVirtual().uncaughtExceptionHandler(uncaughtExceptionHandler).factory());
    this.owner = owner;
    this.maxConcurrency = maxConcurrency;
    if (maxConcurrency < Integer.MAX_VALUE) {
      semaphore = new Semaphore(maxConcurrency);
    } else {
      semaphore = null;
    }

    initMetrics(maxConcurrency, meterRegistry);

  }

  private void acquirePermit() throws InterruptedException {
    if (semaphore != null) {
      semaphore.acquire();
    }
  }

  private boolean tryAcquirePermit() {
    if (semaphore != null) {
      return semaphore.tryAcquire();
    }
    return true;
  }

  private void releasePermit() {
    if (semaphore != null) {
      semaphore.release();
    }
  }

  public void start(String taskName, Task task) {
    try {
      waitingTasks.incrementAndGet();
      acquirePermit();
      doStart(taskName, task);
      waitingTasks.decrementAndGet();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void doStart(String taskName, Task task) {
    if (!executorService.isShutdown()) {
      task.init();
      try {
        executorService.execute(() -> {
          try {
            Thread t = Thread.currentThread();
            t.setName("%s-%s".formatted(owner, taskName));
            tasks.add(task);
            task.run();
          }
          finally {
            releasePermit();
            tasks.remove(task);
          }
        });
      }
      catch (RejectedExecutionException e) {
        logger.warn(
            "Cannot start new task, as task executor is closed and underlying ExecutorService is shutdown.",
            e);
        try {
          task.close();
        }
        catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          logger.error("Interrupted while closing task", e1);
        }
      }
    }
  }

  @Override
  public void execute(Runnable runnable) {
    start(unnamedTaskName(), Task.of(runnable));
  }

  public void start(Task task) {
    start(unnamedTaskName(), task);
  }

  public boolean tryStart(Task task) {
    return tryStart(unnamedTaskName(), task);
  }

  private String unnamedTaskName() {
    return "%s-task-%d".formatted(owner, unnamedTasksCounter.getAndIncrement());
  }

  public boolean tryStart(String taskName, Task task) {
    if (tryAcquirePermit()) {
      doStart(taskName, task);
      return true;
    }
    return false;
  }

  public void stopAllTasks() throws InterruptedException {
    synchronized (tasks) {
      for (Task t : tasks) {
        t.close();
      }
    }
    tasks.clear();
  }

  public void shutdown() {
    close();
  }

  @Override
  public void close() {
    this.executorService.shutdown();
    try {
      stopAllTasks();
      unnamedTasksCounter.set(0);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    finally {
      try {
        if (!this.executorService.awaitTermination(10L, TimeUnit.SECONDS)) {
          this.executorService.shutdownNow();
        }
      }
      catch (InterruptedException e) {
        this.executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
      finally {
        joinLatch.countDown();
      }
    }
  }

  public void join() throws InterruptedException {
    joinLatch.await();
  }

  public int maxConcurrency() {
    return maxConcurrency;
  }

  public int currentTasksCount() {
    return tasks.size();
  }

  private void initMetrics(int maxConcurrency, MeterRegistry meterRegistry) {

    Gauge.builder("task.executor.limited", () -> semaphore == null ? 0 : 1)
        .description("Whether task executor's max concurrency limit is set")
        .tag("owner", this.owner)
        .register(meterRegistry);

    if (semaphore != null) {
      Gauge.builder("task.executor.max.concurrency", () -> maxConcurrency)
          .description("Task executor's max concurrency limit")
          .tag("owner", this.owner)
          .register(meterRegistry);
    }

    Gauge.builder("task.executor.waiting", waitingTasks::get)
        .description("The count of tasks awaiting to be started")
        .tag("owner", this.owner)
        .register(meterRegistry);

    Gauge.builder("task.executor.running", tasks::size)
        .description("The count of tasks currently running")
        .tag("owner", this.owner)
        .register(meterRegistry);
  }

}
