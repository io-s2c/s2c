package io.s2c.concurrency;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

public class GuardedValue<T> {

  private volatile T value;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public GuardedValue(T initialValue) {
    this.value = initialValue;
  }

  public void read(Consumer<T> readConsumer) {
    lock.readLock().lock();
    try {
      readConsumer.accept(value);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public void write(UnaryOperator<T> writeFunction) {
    lock.writeLock().lock();
    try {
      value = writeFunction.apply(value);
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public boolean tryWrite(UnaryOperator<T> writeFunction) {
    boolean locked = lock.writeLock().tryLock();
    if (!locked) {
      return false;
    }
    try {
      value = writeFunction.apply(value);
      return true;
    }
    finally {
      lock.writeLock().unlock();
    }
  }

}