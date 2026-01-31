package io.s2c.concurrency;

import java.util.concurrent.TimeUnit;

/**
 * A class that enables creation of a request which it's response is completed
 * asynchronously. This is mainly used instead of CompletableFutures for many
 * reasons:
 * <ol>
 * <li>Strongly typed and specific exception.</li>
 * <li>Cleaner exception handling, as the exceptions that need to be dealt with,
 * are exactly the ones that might be thrown.</li>
 * <li>Request and response are packed in the same object.</li>
 * <li>Simple blocking await that fits the threading model (Virtual threads)
 * used across the codebase.</li>
 * </ol>
 */
public class RequestResponseTask<G, T, E extends Exception> {

  private final RequestResponse<G, T> requestResponse;
  private boolean finished;
  private E ex;

  public RequestResponseTask(G request) {
    requestResponse = new RequestResponse<>(request);
  }

  public G request() {
    return this.requestResponse.request();
  }

  public void response(T response) {
    this.requestResponse.response(response);
  }

  public T response() {
    return this.requestResponse.response();
  }

  public void exception(E ex) {
    this.ex = ex;
    this.requestResponse.response(null);
  }
  
  public E excption() {
    return this.ex;
  }

  public T await() throws InterruptedException, E {
    return await(1L, TimeUnit.DAYS);
  }
  
  public boolean finished() {
    return finished;
  }

  public T await(long timeout, TimeUnit unit) throws InterruptedException, E {
    T result = requestResponse.await(timeout, unit);
    finished = true;
    if (ex != null) {
      throw ex;
    }
    return result;
  }

}