package io.s2c.util;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.spi.LoggingEventBuilder;

import io.s2c.configs.S2CRetryOptions;

/**
 * A utility class for implementing exponential backoff with jitter for retry mechanisms. The
 * implementation approximates the exponential backoff algorithm pattern used by AWS SDK clients as
 * specified at: https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html This
 * class is not thread-safe.
 */

public class BackoffCounter {

  private final Builder builder;
  private long currentAttempt = 0;
  private long nextDuration = 0;
  private String name = null;
  private volatile boolean canAttempt = true;

  BackoffCounter(Builder builder) {
    this.builder = builder;
    this.nextDuration = calculateDuration(1);
  }

  public static Builder withRetryOptions(S2CRetryOptions retryOptions) {
    return builder().baseDelayMs(retryOptions.baseDelayMS())
        .maxAttempts(retryOptions.maxAttempts())
        .maxBackoffSeconds(retryOptions.maxDelaySeconds());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
    }

    private static final int MAX_BACKOFF_SECONDS = 20;
    private static final int BASE_BACKOFF = 2;
    private static final int MAX_ATTEMPTS = 3;
    private long maxBackoffSeconds = MAX_BACKOFF_SECONDS;
    private long baseDelayMs = BASE_BACKOFF;
    private long maxAttempts = MAX_ATTEMPTS;
    private String name = null;

    public Builder maxBackoffSeconds(long maxBackoffSeconds) {
      this.maxBackoffSeconds = maxBackoffSeconds;
      return this;
    }

    public Builder baseDelayMs(long baseDelayMs) {
      this.baseDelayMs = baseDelayMs;
      return this;
    }

    /**
     * @param maxAttempts: the maximum count of attempts before canAttempt return false. If set to a
     * negative number, attempts are unlimited and canAttempt will always return true.
     */
    public Builder maxAttempts(long maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder unlimited() {
      this.maxAttempts = -1;
      return this;
    }

    public BackoffCounter build() {
      if (maxBackoffSeconds < 0) {
        throw new IllegalArgumentException("maxBackoffSeconds must be > 0");
      }
      if (baseDelayMs < 0) {
        throw new IllegalArgumentException("backoffMultiplier must be > 0");
      }
      return new BackoffCounter(this);
    }

  }

  public long currentAttempt() {
    return this.currentAttempt;
  }

  public void reset() {
    this.currentAttempt = 0;
  }

  public long remainingAttempts() {
    return this.builder.maxAttempts - this.currentAttempt;
  }

  public boolean canAttempt() {
    return canAttempt;
  }

  public long nextWaitMS() {
    return nextDuration;
  }

  public float nextWaitSec() {
    if (nextWaitMS() > 0) {
      return nextWaitMS() / 1000.0f;
    }
    return 0.0f;
  }

  public String nextWaitSecFormatted() {
    return "%.2f".formatted(nextWaitSec());
  }

  public long maxAttempts() {
    return this.builder.maxAttempts;
  }

  public void awaitNextAttempt() throws InterruptedException {
    long attempt = 0;
    long currentDuration = nextDuration;
    // Always await for maxAttempts < -1 (unlimited attempts)
    if (++this.currentAttempt >= this.builder.maxAttempts && this.builder.maxAttempts >= 0) {
      canAttempt = false;
      return;
    }
    attempt = this.currentAttempt;
    nextDuration = calculateDuration(attempt + 1);
    TimeUnit.MILLISECONDS.sleep(currentDuration);
  }

  public LoggingEventBuilder enrich(LoggingEventBuilder builder) {
    Objects.requireNonNull(builder, "'builder' cannot be null");
    builder.addKeyValue("currentAttempt", currentAttempt())
        .addKeyValue("maxAttempts", maxAttempts())
        .addKeyValue("delaySec", nextWaitSecFormatted());
    if (name != null) {
      builder.addKeyValue("name", name);
    }
    return builder;
  }

  private long calculateDuration(long attempt) {
    double jitter = ThreadLocalRandom.current().nextDouble(0.1, 1);
    long maxMS = TimeUnit.SECONDS.toMillis(builder.maxBackoffSeconds);
    return Math.round(Math.min(maxMS, builder.baseDelayMs * (1L << attempt)) * jitter);
  }

}
