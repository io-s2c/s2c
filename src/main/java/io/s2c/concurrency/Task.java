package io.s2c.concurrency;

public interface Task extends Runnable, InterruptableAutoCloseable {

  static Task of(Runnable run) {
    return of(() -> {
    }, run, () -> {
    });
  }

  static Task of(Runnable run, InterruptableAutoCloseable closeable) {
    return of(() -> {
    }, run, closeable);
  }

  void init();

  static Task of(Runnable init, Runnable run, InterruptableAutoCloseable closeable) {
    return new Task() {

      @Override
      public void init() {
        init.run();
      }

      @Override
      public void run() {
        run.run();
      }

      @Override
      public void close() throws InterruptedException {
        closeable.close();
      }
    };
  }
}