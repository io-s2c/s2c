package io.s2c.concurrency;

import java.util.concurrent.locks.ReentrantLock;

public class Sequencer {
  private ReentrantLock lock = new ReentrantLock();
  
  public void sequence(Runnable runnable) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      runnable.run();
    }
    finally {
      lock.unlock();
    }
  }
}
