package io.s2c.configs;

public class S2CRetryOptions {
  
  private static final int MAX_DELAY_SECONDS = 5;
  private static final int BASE_DELAY_MS = 300;
  private static final int THROTTLING_SECONDS = 1;
  private static final int MAX_ATTEMPTS = 3;
  
  private long maxDelaySeconds = 2;
  private long baseDelayMS = 100;
  private long throttlingSeconds = 1;
  private long maxAttempts = 3;
  
  public S2CRetryOptions() { 
    maxDelaySeconds = MAX_DELAY_SECONDS;
    baseDelayMS = BASE_DELAY_MS;
    throttlingSeconds = THROTTLING_SECONDS;
    maxAttempts = MAX_ATTEMPTS;
  }

  public S2CRetryOptions baseDelayMS(long milliseconds) {
    this.baseDelayMS = milliseconds;
    return this;
  }

  public S2CRetryOptions throttlingSeconds(long seconds) {
    this.throttlingSeconds = seconds;
    return this;
  }

  public S2CRetryOptions maxAttempts(long attempts) {
    this.maxAttempts = attempts;
    return this;
  }


  public S2CRetryOptions maxDelaySeconds(long seconds) {
    this.maxDelaySeconds = seconds;
    return this;
  }

  public long maxDelaySeconds() { return this.maxDelaySeconds; }

  public long baseDelayMS() { return this.baseDelayMS; }

  public long throttlingSeconds() { return this.throttlingSeconds; }

  public long maxAttempts() { return this.maxAttempts; }

}
