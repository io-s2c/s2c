package io.s2c;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import io.s2c.StateRequestHandler.TraceableStateRequest;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.model.messages.StateRequest;

public class EOSBuffer implements AutoCloseable {

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition gcLoopCondition = lock.newCondition();

  private final Map<Long, TraceableStateRequest> pendings;
  private long min;
  private final long size;
  private final Consumer<TraceableStateRequest> drainConsumer;
  private final long gcDelaySec;
  
  private volatile boolean gcLoopRunning = true;
  private volatile boolean closed = false;

  private volatile long lastAccess;

  public EOSBuffer(int size,
      long min,
      Consumer<TraceableStateRequest> drainConsumer,
      long gcDelaySec,
      TaskExecutor taskExecutor) {
    this.size = size;
    this.pendings = HashMap.newHashMap(size);
    this.min = min;
    this.drainConsumer = drainConsumer;
    this.gcDelaySec = gcDelaySec;
    taskExecutor.execute(this::startGCLoop);
  }

  private void startGCLoop() {
    long lastCheck = 0;
    while (!closed) {
      lastCheck = lastAccess;
      try {
        TimeUnit.SECONDS.sleep(gcDelaySec);
        if (lock.tryLock()) {
          try {
            while (!gcLoopRunning && !closed) {
              gcLoopCondition.await();
            }
            if (lastCheck == lastAccess) {
              pendings.clear();
              gcLoopRunning = false;
            }
          }
          finally {
            lock.unlock();
          }
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  public boolean add(TraceableStateRequest req) {
    var stateRequest = req.reqRes().request();
    lock.lock();
    try {
      if (closed) {
        throw new IllegalStateException("Cannot add to buffer after it is closed.");
      }
      lastAccess = System.nanoTime();
      if (stateRequest.getSequenceNumber() == min || notTooFar(stateRequest)) {
        acceptAndDrain(req, stateRequest);
        gcLoopRunning = true;
        gcLoopCondition.signal();
        return true;
      }
      return false;
    }
    finally {
      lock.unlock();
    }
  }

  private boolean notTooFar(StateRequest stateRequest) {
    return stateRequest.getSequenceNumber() > min && stateRequest.getSequenceNumber() - min < size;
  }

  private void acceptAndDrain(TraceableStateRequest req, StateRequest stateRequest) {
    pendings.putIfAbsent(stateRequest.getSequenceNumber(), req);
    doDrain();
  }

  private void doDrain() {
    var next = pendings.remove(min);
    while (next != null) {
      drainConsumer.accept(next);
      min++;
      next = pendings.remove(min);
    }
  }

  public void close() {
    lock.lock();
    try {
      pendings.clear();
      closed = true;
      gcLoopRunning = false;
      gcLoopCondition.signal();
    }
    finally {
      lock.unlock();
    }
  }

  public long minSequenceNumber() {
    lock.lock();
    try {
      return this.min;
    }
    finally {
      lock.unlock();
    }
  }
}
