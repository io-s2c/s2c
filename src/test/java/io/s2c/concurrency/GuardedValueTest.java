package io.s2c.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GuardedValueTest {

  private GuardedValue<Integer> guardedValue;

  @BeforeEach
  void setUp() {
    guardedValue = new GuardedValue<>(0);
  }

  @Test
  void testInitialValue() {
    guardedValue.read(value -> assertEquals(0, value));
  }

  @Test
  void testMultipleWrites() throws InterruptedException {
    
    CountDownLatch l = new CountDownLatch(3);
    
    CompletableFuture.runAsync(() -> {
      guardedValue.write(value -> value + 1);
      l.countDown();
    });
    
    CompletableFuture.runAsync(() -> {
      guardedValue.write(value -> value + 1);
      l.countDown();
    });
    
    CompletableFuture.runAsync(() -> {
      guardedValue.write(value -> value + 1);
      l.countDown();
    });
    
    l.await();
    guardedValue.read(value -> assertEquals(3, value));
  }

  @Test
  void testRead() {
    guardedValue.write(value -> 10);
    guardedValue.read(value -> assertEquals(10, value));
  }

  @Test
  void testGuardedObject() {
    GuardedValue<TestObject> objValue = new GuardedValue<>(new TestObject(0));
    objValue.read(obj -> assertEquals(0, obj.value));
    objValue.write(obj -> new TestObject(obj.value + 1));
    objValue.read(obj -> assertEquals(1, obj.value));
  }

  @Test
  void testNullValue() {
    GuardedValue<String> nullValue = new GuardedValue<>(null);
    nullValue.read(value -> assertNull(value));
    
    nullValue.write(value -> "not-null");
    nullValue.read(value -> assertEquals("not-null", value));
  }

  private static class TestObject {
    final int value;
    
    TestObject(int value) {
      this.value = value;
    }
  }
}

