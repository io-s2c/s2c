package io.s2c.concurrency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Awaiter<T, E extends Exception> {

  private static final Object wakeUp = new Object();

  private final Supplier<E> stopExceptionSupplier;
  private volatile boolean stopped = false;
  private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

  public Awaiter(Supplier<E> stopExceptionSupplier) {
    this.stopExceptionSupplier = stopExceptionSupplier;
  }

  public void accept(T t) throws InterruptedException {
    if (!stopped)
      queue.put(t);
  }

  @SuppressWarnings("unchecked")
  public T await(Predicate<T> until) throws InterruptedException, E {
    while (true) {
      if (stopped) {
        throw stopExceptionSupplier.get();
      }
      T value = (T) queue.take();
      if (value != wakeUp && until.test(value)) {
          return value;
        }
    }
  }

  public void stop() throws InterruptedException {
    stopped = true;
    queue.put(wakeUp);
  }
}
