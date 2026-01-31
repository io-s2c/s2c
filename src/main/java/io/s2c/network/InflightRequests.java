package io.s2c.network;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.s2c.concurrency.RequestResponseTask;
import io.s2c.model.messages.S2CMessage;
import io.s2c.network.error.ClientException;

class InflightRequests {

  private static record InflightRequest(
      RequestResponseTask<S2CMessage, S2CMessage, ClientException> request,
      Timer.Sample timerSample) {
  }
  
  private final Map<String, InflightRequest> inFlight = new HashMap<>();
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final MeterRegistry meterRegistry;

  private final Timer requestTimer;

  public InflightRequests(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    requestTimer = Timer.builder("s2c.client.requests.latency").register(meterRegistry);
  }

  public void add(RequestResponseTask<S2CMessage, S2CMessage, ClientException> request) {
    lock.writeLock().lock();
    try {
      Timer.Sample timerSample = Timer.start(meterRegistry);
      inFlight.put(request.request().getCorrelationId(), new InflightRequest(request, timerSample));
    }
    finally {
      lock.writeLock().unlock();
    }
  }
  
  public RequestResponseTask<S2CMessage, S2CMessage, ClientException> get(String correlationId) {
    lock.readLock().lock();
    try {
      InflightRequest req = inFlight.get(correlationId);
      if (req == null) return null;
      return req.request();
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public void discard(String correlationId) {
    lock.writeLock().lock();
    try {
      // discard might be called on ClientException before the request was put inflight
      var req = inFlight.get(correlationId);
      if (req != null) {
        req.timerSample().stop(requestTimer);
      }
    }
    finally {
      lock.writeLock().unlock();
    }
  }
  public void discardAndRemove(String correlationId) {
    lock.writeLock().lock();
    try {
      discard(correlationId);
      inFlight.remove(correlationId);
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public void respondToRequest(S2CMessage res) {
    lock.writeLock().lock();
    try {
      inFlight.computeIfPresent(res.getCorrelationId(), (k, v) -> {
        v.timerSample().stop(requestTimer);
        if (v.request.response() == null && v.request.excption() == null) {
          v.request.response(res);
        }
        return v; 
      });
    }
    finally {
      lock.writeLock().unlock();
    }

  }

  public void fail(ClientException e) {
    lock.writeLock().lock();
    try {
      lock.writeLock().lock();
      try {
        var iterator = inFlight.entrySet().iterator();
        while (iterator.hasNext()) {
          iterator.next().getValue().request().exception(e);
          // Cleared by exception handling in sendAndAwaitResponse
        }
      }
      finally {
        lock.writeLock().unlock();
      }    }
    finally {
      lock.writeLock().unlock();
    }
  }
  
  public void clear() {
    lock.writeLock().lock();
    try {
      var iterator = inFlight.entrySet().iterator();
      while (iterator.hasNext()) {
        discard(iterator.next().getKey());
        iterator.remove();
      }
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public int size() {
    lock.readLock().lock();
    try {
      return inFlight.size();
    }
    finally {
      lock.readLock().unlock();
    }
  }

}