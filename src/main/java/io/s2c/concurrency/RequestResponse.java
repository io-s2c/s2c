package io.s2c.concurrency;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RequestResponse<G, T> {

  private CountDownLatch latch = new CountDownLatch(1);
  private T response;
  private final G request;
  private AtomicBoolean completed = new AtomicBoolean();

  public RequestResponse(G request) {
    this.request = request;
  }

  public G request() {
    return this.request;
  }

  public T response() {
    return response;
  }

  public void response(T response) {
    if (completed.compareAndSet(false, true)) {
      this.response = response;
      latch.countDown();
    }
  }

  public T await() throws InterruptedException {
    latch.await();
    return response;
  }

  public T await(long timeout, TimeUnit unit) throws InterruptedException {
    latch.await(timeout, unit);
    return response;
  }

}