package io.s2c.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.s2c.configs.S2CRetryOptions;

class BackoffCounterTest {

  private BackoffCounter backoffCounter;

  @BeforeEach
  void setUp() {
    backoffCounter = BackoffCounter.builder()
        .baseDelayMs(100)
        .maxBackoffSeconds(5)
        .maxAttempts(5)
        .build();
  }

  @Test
  void testInitialState() {
    assertEquals(0, backoffCounter.currentAttempt());
    assertEquals(5, backoffCounter.maxAttempts());
    assertEquals(5, backoffCounter.remainingAttempts());
    assertTrue(backoffCounter.canAttempt());
    assertTrue(backoffCounter.nextWaitMS() > 0);
  }

  @Test
  void testReset() throws InterruptedException {
    backoffCounter.awaitNextAttempt();
    assertEquals(1, backoffCounter.currentAttempt());
    backoffCounter.reset();
    assertEquals(0, backoffCounter.currentAttempt());
    assertEquals(5, backoffCounter.remainingAttempts());
  }

  @Test
  void testRemainingAttempts() throws InterruptedException {
    assertEquals(5, backoffCounter.remainingAttempts());

    backoffCounter.awaitNextAttempt();
    assertEquals(4, backoffCounter.remainingAttempts());

    backoffCounter.awaitNextAttempt();
    assertEquals(3, backoffCounter.remainingAttempts());
  }

  @Test
  void testMaxAttemptsReached() throws InterruptedException {
    for (int i = 0; i < 5; i++) {
      assertTrue(backoffCounter.canAttempt());
      backoffCounter.awaitNextAttempt();
    }

    assertFalse(backoffCounter.canAttempt());
    assertEquals(0, backoffCounter.remainingAttempts());
  }

  @Test
  void testUnlimitedAttempts() throws InterruptedException {
    BackoffCounter unlimited = BackoffCounter.builder()
        .baseDelayMs(10)
        .maxBackoffSeconds(1)
        .maxAttempts(-1)
        .build();

    for (int i = 0; i < 10; i++) {
      assertTrue(unlimited.canAttempt());
      unlimited.awaitNextAttempt();
    }

    assertTrue(unlimited.canAttempt());
  }

  @Test
  void testMaxBackoffCap() throws InterruptedException {
    BackoffCounter capped = BackoffCounter.builder()
        .baseDelayMs(1000)
        .maxBackoffSeconds(1) // 1 second max
        .maxAttempts(10)
        .build();

    for (int i = 0; i < 5; i++) {
      long waitMs = capped.nextWaitMS();
      assertTrue(waitMs <= 2000); // Should not exceed 2 seconds (with some margin for jitter)
      capped.awaitNextAttempt();
    }
  }

  @Test
  void testNextWaitSec() {
    float waitSec = backoffCounter.nextWaitSec();
    assertTrue(waitSec >= 0);
    assertTrue(waitSec < 1.0); // Initial wait should be less than 1 second
  }

  @Test
  void testNextWaitSecFormatted() {
    String formatted = backoffCounter.nextWaitSecFormatted();
    assertTrue(formatted.matches("0\\.[0-9]{2}"));
  }

  @Test
  void testBuilderValidation() {
    assertThrows(IllegalArgumentException.class, () -> {
      BackoffCounter.builder().maxBackoffSeconds(-1).build();
    });

    assertThrows(IllegalArgumentException.class, () -> {
      BackoffCounter.builder().baseDelayMs(-1).build();
    });
  }

  @Test
  void testWithRetryOptions() {
    S2CRetryOptions retryOptions = new S2CRetryOptions()
        .baseDelayMS(200)
        .maxAttempts(3)
        .maxDelaySeconds(10);
    BackoffCounter counter = BackoffCounter.withRetryOptions(retryOptions).build();

    assertEquals(3, counter.maxAttempts());
    assertEquals(3, counter.remainingAttempts());
  }

  @Test
  void testInitialJitteredBackoffIsWithinExpectedRange() {

    for (int i = 0; i < 10; i++) {
      BackoffCounter counter = BackoffCounter.builder()
          .baseDelayMs(1000)
          .maxBackoffSeconds(10)
          .maxAttempts(1)
          .build();

      long wait = counter.nextWaitMS();

      // attempt = 1 -> baseDelayMs * 2 = 2000ms
      // jitter in range [0.1, 1.0)
      long minExpected = 200; // 2000 * 0.1
      long maxExpected = 2000; // 2000 * 1.0 (exclusive, but safe upper bound)

      assertTrue(wait >= minExpected);

      assertTrue(wait <= maxExpected);
      
    }
  }

}
