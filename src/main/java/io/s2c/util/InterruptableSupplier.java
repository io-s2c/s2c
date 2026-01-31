package io.s2c.util;

@FunctionalInterface
public interface InterruptableSupplier<T> {
  public T get() throws InterruptedException;
}
