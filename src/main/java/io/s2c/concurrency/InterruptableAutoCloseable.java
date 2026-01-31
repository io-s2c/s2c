package io.s2c.concurrency;

public interface InterruptableAutoCloseable extends AutoCloseable {

  void close() throws InterruptedException;

}